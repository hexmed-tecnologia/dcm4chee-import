import csv
import json
import math
import os
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from urllib import error as urlerror
from urllib import request as urlrequest


CSV_SEP = ";"
APP_DISPLAY_NAME = "DICOM Multi Toolkit"

DCM4CHE_STORE_RQ_RE = re.compile(
    r"<<\s+\d+:C-STORE-RQ\[[\s\S]*?iuid=([0-9]+(?:\.[0-9]+)+)\s+-",
    re.IGNORECASE,
)
DCM4CHE_STORE_RSP_OK_RE = re.compile(
    r">>\s+\d+:C-STORE-RSP\[[\s\S]*?status=0H[\s\S]*?iuid=([0-9]+(?:\.[0-9]+)+)\s+-",
    re.IGNORECASE,
)
DCM4CHE_STORE_RSP_ERR_RE = re.compile(
    r">>\s+\d+:C-STORE-RSP\[[\s\S]*?status=(?!0H)([A-F0-9]+H)[\s\S]*?iuid=([0-9]+(?:\.[0-9]+)+)\s+-",
    re.IGNORECASE,
)

DCMTK_SENDING_FILE_RE = re.compile(r"I:\s+Sending file:\s+(.+)$")
DCMTK_BAD_FILE_RE = re.compile(r"E:\s+Bad DICOM file:\s+(.+?):\s*(.+)$")
DCMTK_STORE_RSP_RE = re.compile(r"I:\s+Received Store Response\s+\((.+)\)$")
UID_TAG_0008_0018 = re.compile(r"\(0008,0018\)[^\[]*\[([^\]]*)\]", re.IGNORECASE)
UID_TAG_0002_0010 = re.compile(r"\(0002,0010\)[^\[]*\[([^\]]*)\]", re.IGNORECASE)
UID_VALUE_RE = re.compile(r"[0-9]+(?:\.[0-9]+)+")
IS_WINDOWS = os.name == "nt"


def hidden_process_kwargs() -> dict:
    if not IS_WINDOWS:
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {"startupinfo": startupinfo, "creationflags": subprocess.CREATE_NO_WINDOW}


@dataclass
class AppConfig:
    toolkit: str = "dcm4che"
    # Internal runtime values (always resolved from local "toolkits" folder).
    dcm4che_bin_path: str = ""
    dcmtk_bin_path: str = ""
    aet_origem: str = "HMD_IMPORTER"
    aet_destino: str = "HMD_IMPORTED"
    pacs_host: str = "192.168.1.70"
    pacs_port: int = 5555
    pacs_rest_host: str = "192.168.1.70:8080"
    runs_base_dir: str = ""
    batch_size_default: int = 200
    nivel_log_minimo: str = "INFO"
    allowed_extensions_csv: str = ".dcm"
    restrict_extensions: bool = True
    include_no_extension: bool = True
    collect_size_bytes: bool = False
    ts_mode: str = "AUTO"
    dcm4che_send_mode: str = "MANIFEST_FILES"
    # Internal flag: keep Windows-stable wrapper for .bat execution by default.
    dcm4che_use_shell_wrapper: bool = True


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def now_br() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def now_dual_timestamp() -> tuple[str, str]:
    dt = datetime.now()
    return dt.strftime("%d/%m/%Y %H:%M:%S"), dt.strftime("%Y-%m-%dT%H:%M:%S")


def now_run_id() -> str:
    return datetime.now().strftime("%d%m%Y_%H%M%S")


def sanitize_uid(value: str) -> str:
    m = UID_VALUE_RE.search((value or "").strip())
    return m.group(0).strip() if m else ""


def looks_like_dicom_payload_file(file_path: Path) -> bool:
    name_up = file_path.name.upper()
    if name_up == "DICOMDIR":
        return False
    ext = file_path.suffix.lower()
    if ext in [".dcm", ".dicom", ".ima"]:
        return True
    if not ext and sanitize_uid(file_path.name):
        return True
    return False


def normalize_dcm4che_send_mode(mode: str) -> str:
    m = (mode or "").strip().upper()
    if m in ["FILES", "MANIFEST_FILES"]:
        return "MANIFEST_FILES"
    return "FOLDERS"


def toolkit_run_suffix(toolkit: str, dcm4che_send_mode: str = "MANIFEST_FILES") -> str:
    t = (toolkit or "").strip().lower()
    if t == "dcm4che":
        return "dcm4che_files" if normalize_dcm4che_send_mode(dcm4che_send_mode) == "MANIFEST_FILES" else "dcm4che_folders"
    if t == "dcmtk":
        return "dcmtk"
    return re.sub(r"[^a-z0-9]+", "_", t).strip("_") or "toolkit"


def strip_known_run_suffixes(run_id: str) -> str:
    base = (run_id or "").strip()
    if not base:
        return base
    known_suffixes = [
        "_dcm4che_folders",
        "_dcm4che_files",
        "_dcm4che",
        "_dcmtk",
    ]
    changed = True
    while changed:
        changed = False
        lower = base.lower()
        for suffix in known_suffixes:
            if lower.endswith(suffix):
                base = base[: -len(suffix)].rstrip("_")
                changed = True
                break
    return base


def send_checkpoint_filename(cfg: AppConfig) -> str:
    toolkit = (cfg.toolkit or "").strip().lower()
    if toolkit == "dcm4che":
        mode = normalize_dcm4che_send_mode(cfg.dcm4che_send_mode)
        mode_suffix = "files" if mode == "MANIFEST_FILES" else "folders"
        return f"send_checkpoint_dcm4che_{mode_suffix}.json"
    if toolkit == "dcmtk":
        return "send_checkpoint_dcmtk.json"
    return f"send_checkpoint_{toolkit_run_suffix(cfg.toolkit, cfg.dcm4che_send_mode)}.json"


def format_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "calculando"
    total = int(seconds)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


class WorkflowCancelled(Exception):
    pass


def read_app_version(base_dir: Path) -> str:
    candidates = [base_dir / "VERSION", base_dir.parent / "VERSION"]
    for p in candidates:
        try:
            if p.exists():
                raw = p.read_text(encoding="utf-8", errors="replace").strip()
                if raw:
                    return raw
        except Exception:
            pass
    return "v0.0.0-dev"


def parse_extensions(value: str) -> set[str]:
    out: set[str] = set()
    for token in (value or "").split(","):
        t = token.strip().lower()
        if not t:
            continue
        if not t.startswith("."):
            t = "." + t
        out.add(t)
    return out


def write_csv_row(path: Path, row: dict, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    active_fields = list(fieldnames)
    if write_header:
        if "timestamp_br" not in active_fields:
            active_fields.append("timestamp_br")
        if "timestamp_iso" not in active_fields:
            active_fields.append("timestamp_iso")
    else:
        # Keep compatibility when appending to older CSV schemas.
        with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
            first = f.readline().strip()
        if first:
            active_fields = next(csv.reader([first], delimiter=CSV_SEP))
    row_data = dict(row)
    if "timestamp_br" in active_fields and "timestamp_br" not in row_data:
        ts_br, ts_iso = now_dual_timestamp()
        row_data["timestamp_br"] = ts_br
        row_data["timestamp_iso"] = ts_iso
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=active_fields, delimiter=CSV_SEP)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row_data.get(k, "") for k in active_fields})


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f, delimiter=CSV_SEP))


def write_csv_table(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=CSV_SEP)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


RUN_SUBDIR_CORE = "core"
RUN_SUBDIR_TELEMETRY = "telemetry"
RUN_SUBDIR_REPORTS = "reports"

RUN_ARTIFACT_SUBDIR: dict[str, str] = {
    "manifest_folders.csv": RUN_SUBDIR_CORE,
    "manifest_files.csv": RUN_SUBDIR_CORE,
    "analysis_summary.csv": RUN_SUBDIR_CORE,
    "send_results_by_file.csv": RUN_SUBDIR_CORE,
    "send_summary.csv": RUN_SUBDIR_CORE,
    "validation_results.csv": RUN_SUBDIR_CORE,
    # Legacy core files (kept only for cleanup/fallback compatibility).
    "file_iuid_map.csv": RUN_SUBDIR_CORE,
    "validation_by_iuid.csv": RUN_SUBDIR_CORE,
    "validation_by_file.csv": RUN_SUBDIR_CORE,
    "send_checkpoint.json": RUN_SUBDIR_CORE,
    "send_checkpoint_dcm4che_folders.json": RUN_SUBDIR_CORE,
    "send_checkpoint_dcm4che_files.json": RUN_SUBDIR_CORE,
    "send_checkpoint_dcmtk.json": RUN_SUBDIR_CORE,
    "events.csv": RUN_SUBDIR_TELEMETRY,
    # Legacy telemetry files (kept only for cleanup/fallback compatibility).
    "analysis_events.csv": RUN_SUBDIR_TELEMETRY,
    "send_events.csv": RUN_SUBDIR_TELEMETRY,
    "send_errors.csv": RUN_SUBDIR_TELEMETRY,
    "consistency_events.csv": RUN_SUBDIR_TELEMETRY,
    "storescu_execucao.log": RUN_SUBDIR_TELEMETRY,
    "reconciliation_report.csv": RUN_SUBDIR_REPORTS,
    "validation_full_report_A.csv": RUN_SUBDIR_REPORTS,
    "validation_full_report_C.csv": RUN_SUBDIR_REPORTS,
}


def run_artifact_variants(run_dir: Path, filename: str) -> tuple[Path, Path]:
    subdir = RUN_ARTIFACT_SUBDIR.get(filename, RUN_SUBDIR_CORE)
    categorized_path = run_dir / subdir / filename
    legacy_path = run_dir / filename
    return categorized_path, legacy_path


def resolve_run_artifact_path(
    run_dir: Path,
    filename: str,
    *,
    for_write: bool,
    logger=None,
    keep_legacy_on_write: bool = True,
) -> Path:
    categorized_path, legacy_path = run_artifact_variants(run_dir, filename)
    source = "categorized_default"
    chosen = categorized_path
    if for_write:
        if categorized_path.exists():
            chosen = categorized_path
            source = "categorized_existing"
        elif keep_legacy_on_write and legacy_path.exists():
            chosen = legacy_path
            source = "legacy_existing"
        chosen.parent.mkdir(parents=True, exist_ok=True)
    else:
        if categorized_path.exists():
            chosen = categorized_path
            source = "categorized_existing"
        elif legacy_path.exists():
            chosen = legacy_path
            source = "legacy_existing"
    if logger:
        logger(f"[RUN_PATH_RESOLVE] mode={'write' if for_write else 'read'} file={filename} source={source} path={chosen}")
    return chosen


def cleanup_run_artifact_variants(run_dir: Path, filename: str) -> None:
    categorized_path, legacy_path = run_artifact_variants(run_dir, filename)
    for p in [categorized_path, legacy_path]:
        if p.exists():
            p.unlink()


def resolve_run_batch_args_dir(run_dir: Path, *, for_write: bool, logger=None) -> Path:
    categorized_dir = run_dir / RUN_SUBDIR_CORE / "batch_args"
    legacy_dir = run_dir / "batch_args"
    source = "categorized_default"
    chosen = categorized_dir
    if for_write:
        if categorized_dir.exists():
            chosen = categorized_dir
            source = "categorized_existing"
        elif legacy_dir.exists():
            chosen = legacy_dir
            source = "legacy_existing"
        chosen.mkdir(parents=True, exist_ok=True)
    else:
        if categorized_dir.exists():
            chosen = categorized_dir
            source = "categorized_existing"
        elif legacy_dir.exists():
            chosen = legacy_dir
            source = "legacy_existing"
    if logger:
        logger(f"[RUN_PATH_RESOLVE] mode={'write' if for_write else 'read'} file=batch_args source={source} path={chosen}")
    return chosen


def write_telemetry_event(path: Path, run_id: str, event_type: str, message: str, ref: str = "") -> None:
    fields = ["run_id", "event_type", "timestamp_iso", "message", "ref"]
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=CSV_SEP)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "run_id": run_id,
                "event_type": event_type,
                "timestamp_iso": now_iso(),
                "message": message,
                "ref": ref,
            }
        )


def build_iuid_map_from_send_rows(send_rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in send_rows:
        fp = str(row.get("file_path", "")).strip()
        iuid = str(row.get("sop_instance_uid", "")).strip()
        if not fp or not iuid:
            continue
        out[fp] = {
            "sop_instance_uid": iuid,
            "source_ts_uid": str(row.get("source_ts_uid", "")).strip(),
            "source_ts_name": str(row.get("source_ts_name", "")).strip(),
            "extract_status": str(row.get("extract_status", "")).strip(),
        }
    return out


def merge_iuid_map_from_legacy_file(map_by_file: dict[str, dict], legacy_map_path: Path) -> None:
    if not legacy_map_path.exists():
        return
    for row in read_csv_rows(legacy_map_path):
        fp = str(row.get("file_path", "")).strip()
        iuid = str(row.get("sop_instance_uid", "")).strip()
        if not fp or not iuid or fp in map_by_file:
            continue
        map_by_file[fp] = {
            "sop_instance_uid": iuid,
            "source_ts_uid": str(row.get("source_ts_uid", "")).strip(),
            "source_ts_name": str(row.get("source_ts_name", "")).strip(),
            "extract_status": str(row.get("extract_status", "")).strip(),
        }


def apply_send_result_updates(send_results_path: Path, run_id: str, updates_by_file: dict[str, dict]) -> int:
    if not updates_by_file or not send_results_path.exists():
        return 0
    rows = read_csv_rows(send_results_path)
    if not rows:
        return 0

    changed_rows = 0
    update_keys = ["sop_instance_uid", "source_ts_uid", "source_ts_name", "extract_status"]
    for row in rows:
        if str(row.get("run_id", "")).strip() != run_id:
            continue
        fp = str(row.get("file_path", "")).strip()
        if not fp:
            continue
        upd = updates_by_file.get(fp)
        if not upd:
            continue
        row_changed = False
        for key in update_keys:
            new_val = str(upd.get(key, "")).strip()
            if not new_val:
                continue
            if str(row.get(key, "")).strip() != new_val:
                row[key] = new_val
                row_changed = True
        if row_changed:
            changed_rows += 1

    if changed_rows > 0:
        fieldnames = list(rows[0].keys())
        for key in update_keys:
            if key not in fieldnames:
                fieldnames.append(key)
        write_csv_table(send_results_path, rows, fieldnames)
    return changed_rows


def find_toolkit_bin(base_dir: Path, toolkit_prefix: str, filename: str) -> str:
    toolkits_dir = base_dir / "toolkits"
    if not toolkits_dir.exists():
        return ""
    candidates = [p for p in toolkits_dir.iterdir() if p.is_dir() and p.name.lower().startswith(toolkit_prefix.lower())]
    candidates.sort(reverse=True)
    for cand in candidates:
        bin_dir = cand / "bin"
        if (bin_dir / filename).exists():
            return str(bin_dir)
    return ""


def apply_internal_toolkit_paths(cfg: AppConfig, base_dir: Path, logger=None) -> AppConfig:
    dcm4che_bin = find_toolkit_bin(base_dir, "dcm4che", "storescu.bat")
    dcmtk_bin = find_toolkit_bin(base_dir, "dcmtk", "storescu.exe")
    cfg.dcm4che_bin_path = dcm4che_bin
    cfg.dcmtk_bin_path = dcmtk_bin
    if logger:
        logger(
            f"[TOOLKIT_RESOLVE] toolkit=dcm4che source=internal status={'OK' if dcm4che_bin else 'NOT_FOUND'} "
            f"path={dcm4che_bin or '<missing>'}"
        )
        logger(
            f"[TOOLKIT_RESOLVE] toolkit=dcmtk source=internal status={'OK' if dcmtk_bin else 'NOT_FOUND'} "
            f"path={dcmtk_bin or '<missing>'}"
        )
    return cfg


class ToolkitDriver:
    toolkit_name = "base"

    def storescu_cmd(self, cfg: AppConfig, batch_files: list[Path], args_file: Path) -> list[str]:
        raise NotImplementedError

    def echo_cmd(self, cfg: AppConfig) -> list[str]:
        raise NotImplementedError

    def extract_metadata(self, cfg: AppConfig, file_path: Path) -> tuple[str, str, str, str]:
        raise NotImplementedError

    def parse_send_output(self, lines: list[str], batch_files: list[Path]) -> dict[str, dict]:
        raise NotImplementedError

    def dcmdump_text(self, cmd: list[str]) -> str:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False, **hidden_process_kwargs())
        return ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()


class Dcm4cheDriver(ToolkitDriver):
    toolkit_name = "dcm4che"

    def storescu_cmd(self, cfg: AppConfig, batch_files: list[Path], args_file: Path) -> list[str]:
        if not cfg.dcm4che_bin_path:
            raise RuntimeError(
                "storescu.bat nao encontrado na toolkit interna. "
                "Estrutura esperada: <app>\\toolkits\\dcm4che-*\\bin\\storescu.bat"
            )
        storescu = Path(cfg.dcm4che_bin_path) / "storescu.bat"
        if not storescu.exists():
            raise RuntimeError(f"storescu.bat nao encontrado: {storescu}")
        base = [
            str(storescu),
            "-c",
            f"{cfg.aet_destino}@{cfg.pacs_host}:{cfg.pacs_port}",
        ]
        base.extend([str(p) for p in batch_files])
        if cfg.dcm4che_use_shell_wrapper:
            return ["cmd", "/c", *base]
        # Experimental path: run .bat directly without cmd wrapper.
        return base

    def echo_cmd(self, cfg: AppConfig) -> list[str]:
        if not cfg.dcm4che_bin_path:
            raise RuntimeError(
                "storescu.bat nao encontrado na toolkit interna. "
                "Estrutura esperada: <app>\\toolkits\\dcm4che-*\\bin\\storescu.bat"
            )
        storescu = Path(cfg.dcm4che_bin_path) / "storescu.bat"
        if not storescu.exists():
            raise RuntimeError(f"storescu.bat nao encontrado: {storescu}")
        return ["cmd", "/c", str(storescu), "-c", f"{cfg.aet_destino}@{cfg.pacs_host}:{cfg.pacs_port}"]

    def extract_metadata(self, cfg: AppConfig, file_path: Path) -> tuple[str, str, str, str]:
        if not cfg.dcm4che_bin_path:
            return "", "", "", "dcmdump.bat nao encontrado na toolkit interna"
        dcmdump = Path(cfg.dcm4che_bin_path) / "dcmdump.bat"
        if not dcmdump.exists():
            return "", "", "", "dcmdump.bat nao encontrado"
        out = self.dcmdump_text(["cmd", "/c", str(dcmdump), str(file_path)])
        iuid_m = UID_TAG_0008_0018.search(out)
        ts_m = UID_TAG_0002_0010.search(out)
        iuid = sanitize_uid(iuid_m.group(1) if iuid_m else "")
        ts_uid = sanitize_uid(ts_m.group(1) if ts_m else "")
        return iuid, ts_uid, ts_uid, ""

    def parse_send_output(self, lines: list[str], batch_files: list[Path]) -> dict[str, dict]:
        blob = "\n".join(lines)
        rq_iuids = [x.strip() for x in DCM4CHE_STORE_RQ_RE.findall(blob) if x.strip()]
        ok_iuids = [x.strip() for x in DCM4CHE_STORE_RSP_OK_RE.findall(blob) if x.strip()]
        err_matches = DCM4CHE_STORE_RSP_ERR_RE.findall(blob)
        err_iuids = [uid.strip() for _status, uid in err_matches if uid.strip()]
        err_status_by_iuid = {uid.strip(): status.strip() for status, uid in err_matches if uid.strip()}
        return {
            "__batch__": {
                "rq_iuids": rq_iuids,
                "ok_iuids": ok_iuids,
                "err_iuids": err_iuids,
                "err_status_by_iuid": err_status_by_iuid,
            }
        }


class DcmtkDriver(ToolkitDriver):
    toolkit_name = "dcmtk"

    def storescu_cmd(self, cfg: AppConfig, batch_files: list[Path], args_file: Path) -> list[str]:
        if not cfg.dcmtk_bin_path:
            raise RuntimeError(
                "storescu.exe nao encontrado na toolkit interna. "
                "Estrutura esperada: <app>\\toolkits\\dcmtk-*\\bin\\storescu.exe"
            )
        storescu = Path(cfg.dcmtk_bin_path) / "storescu.exe"
        if not storescu.exists():
            raise RuntimeError(f"storescu.exe nao encontrado: {storescu}")
        return [
            str(storescu),
            "-v",
            "-nh",
            "-aet",
            cfg.aet_origem,
            "-aec",
            cfg.aet_destino,
            cfg.pacs_host,
            str(cfg.pacs_port),
            f"@{args_file}",
        ]

    def echo_cmd(self, cfg: AppConfig) -> list[str]:
        if not cfg.dcmtk_bin_path:
            raise RuntimeError(
                "echoscu.exe nao encontrado na toolkit interna. "
                "Estrutura esperada: <app>\\toolkits\\dcmtk-*\\bin\\echoscu.exe"
            )
        echoscu = Path(cfg.dcmtk_bin_path) / "echoscu.exe"
        if not echoscu.exists():
            raise RuntimeError(f"echoscu.exe nao encontrado: {echoscu}")
        return [str(echoscu), "-aet", cfg.aet_origem, "-aec", cfg.aet_destino, cfg.pacs_host, str(cfg.pacs_port)]

    def extract_metadata(self, cfg: AppConfig, file_path: Path) -> tuple[str, str, str, str]:
        if not cfg.dcmtk_bin_path:
            return "", "", "", "dcmdump.exe nao encontrado na toolkit interna"
        dcmdump = Path(cfg.dcmtk_bin_path) / "dcmdump.exe"
        if not dcmdump.exists():
            return "", "", "", "dcmdump.exe nao encontrado"
        out = self.dcmdump_text([str(dcmdump), "+P", "0008,0018", "+P", "0002,0010", str(file_path)])
        iuid_m = UID_TAG_0008_0018.search(out)
        ts_m = UID_TAG_0002_0010.search(out)
        iuid = iuid_m.group(1).strip() if iuid_m else ""
        ts_uid = ts_m.group(1).strip() if ts_m else ""
        return iuid, ts_uid, ts_uid, ""

    def parse_send_output(self, lines: list[str], batch_files: list[Path]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        current_file = ""
        for line in lines:
            m_file = DCMTK_SENDING_FILE_RE.search(line)
            if m_file:
                current_file = m_file.group(1).strip()
                result.setdefault(current_file, {"send_status": "SENT_UNKNOWN", "status_detail": ""})
                continue
            m_bad = DCMTK_BAD_FILE_RE.search(line)
            if m_bad:
                bad_file = m_bad.group(1).strip()
                detail = m_bad.group(2).strip()
                result[bad_file] = {"send_status": "NON_DICOM", "status_detail": detail}
                continue
            m_rsp = DCMTK_STORE_RSP_RE.search(line)
            if m_rsp and current_file:
                detail = m_rsp.group(1).strip()
                status = "SENT_OK" if "Success" in detail else "SEND_FAIL"
                if ("Unknown Status: 0x110" in detail) and Path(current_file).name.upper() == "DICOMDIR":
                    status = "UNSUPPORTED_DICOM_OBJECT"
                result[current_file] = {"send_status": status, "status_detail": detail}
        for p in batch_files:
            k = str(p)
            result.setdefault(k, {"send_status": "SENT_UNKNOWN", "status_detail": ""})
        return result


def get_driver(toolkit: str) -> ToolkitDriver:
    if toolkit == "dcmtk":
        return DcmtkDriver()
    return Dcm4cheDriver()


class AnalyzeWorkflow:
    def __init__(self, cfg: AppConfig, logger, cancel_event: threading.Event, progress_callback=None):
        self.cfg = cfg
        self.logger = logger
        self.cancel_event = cancel_event
        self.progress_callback = progress_callback

    def _log(self, msg: str) -> None:
        self.logger(msg)

    def _progress(self, text: str) -> None:
        if self.progress_callback:
            self.progress_callback(text)

    def _resolve_runs_base(self, script_dir: Path) -> Path:
        if self.cfg.runs_base_dir.strip():
            p = Path(self.cfg.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (script_dir / p).resolve()
        return (script_dir / "runs").resolve()

    def _with_toolkit_suffix(self, run_id: str) -> str:
        suffix = toolkit_run_suffix(self.cfg.toolkit, self.cfg.dcm4che_send_mode)
        raw = (run_id or "").strip()
        if not raw:
            return raw
        base = strip_known_run_suffixes(raw)
        if base != raw:
            self._log(f"[RUN_ID_GUARD] run_id_normalized from={raw} to={base}_{suffix}")
        if base.lower().endswith(f"_{suffix}"):
            return base
        return f"{base}_{suffix}"

    def run_analysis(self, exam_root: str, batch_size: int, run_id: str = "") -> dict:
        script_dir = Path(__file__).resolve().parent
        runs_base = self._resolve_runs_base(script_dir)
        runs_base.mkdir(parents=True, exist_ok=True)
        run = run_id.strip() or now_run_id()
        run = self._with_toolkit_suffix(run)
        run_dir = runs_base / run
        run_dir.mkdir(parents=True, exist_ok=True)
        self._log("[RUN_LAYOUT] mode=analysis layout=core|telemetry|reports")

        root = Path(exam_root).resolve()
        if not root.exists():
            raise RuntimeError(f"Pasta nao encontrada: {root}")
        if batch_size < 1:
            raise RuntimeError("batch_size deve ser >= 1")

        for filename in ["manifest_folders.csv", "manifest_files.csv", "analysis_summary.csv", "events.csv"]:
            cleanup_run_artifact_variants(run_dir, filename)
        for legacy_name in ["analysis_events.csv", "send_events.csv", "send_errors.csv", "consistency_events.csv"]:
            cleanup_run_artifact_variants(run_dir, legacy_name)
        manifest_folders = resolve_run_artifact_path(run_dir, "manifest_folders.csv", for_write=True, logger=self._log)
        manifest_files = resolve_run_artifact_path(run_dir, "manifest_files.csv", for_write=True, logger=self._log)
        summary = resolve_run_artifact_path(run_dir, "analysis_summary.csv", for_write=True, logger=self._log)
        events = resolve_run_artifact_path(run_dir, "events.csv", for_write=True, logger=self._log)

        allowed_ext = parse_extensions(self.cfg.allowed_extensions_csv)
        include_no_ext = bool(self.cfg.include_no_extension)
        restrict_extensions = bool(self.cfg.restrict_extensions)
        dcm4che_send_mode = normalize_dcm4che_send_mode(self.cfg.dcm4che_send_mode)
        force_all_files_for_folders = self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS"
        if force_all_files_for_folders:
            restrict_extensions = False
        self._log(f"[AN_START] run_id={run} toolkit={self.cfg.toolkit} dcm4che_mode={dcm4che_send_mode}")
        self._log("Iniciando descoberta de arquivos...")
        if force_all_files_for_folders:
            self._log("[AN_FILTER_MODE] mode=all_files reason=dcm4che_folders include_no_extension=IGNORED")
        elif restrict_extensions:
            ext_text = ",".join(sorted(allowed_ext)) if allowed_ext else "<nenhuma_extensao>"
            self._log(
                f"[AN_FILTER_MODE] mode=extensions allowed={ext_text} "
                f"include_no_extension={'ON' if include_no_ext else 'OFF'}"
            )
        else:
            self._log("[AN_FILTER_MODE] mode=all_files include_no_extension=IGNORED")
        self._log(
            f"[AN_SCAN_CONFIG] collect_size_bytes={'ON' if self.cfg.collect_size_bytes else 'OFF'} "
            "(OFF melhora performance em arvores muito grandes)"
        )
        self._progress("progresso analise: preparando varredura...")

        folder_agg: dict[str, dict] = {}
        total_files = 0
        total_bytes = 0
        selected_files = 0
        selected_bytes = 0
        excluded_files = 0
        selected_folder_keys: set[str] = set()

        file_fields = [
            "run_id",
            "seq",
            "file_path",
            "folder_path",
            "extension",
            "size_bytes",
            "selected_for_send",
            "selection_reason",
            "dicom_status",
            "discovered_at",
        ]
        seq = 0
        manifest_files.parent.mkdir(parents=True, exist_ok=True)
        file_output_fields = [*file_fields, "timestamp_br", "timestamp_iso"]
        progress_interval_sec = 2.0
        buffer_size = 2000
        row_buffer: list[dict] = []
        start_ts = time.monotonic()
        last_progress_ts = start_ts
        dirs_processed = 0
        dirs_discovered = 1
        dir_stack: list[Path] = [root]
        scan_errors = 0

        with manifest_files.open("w", newline="", encoding="utf-8") as f_manifest:
            manifest_writer = csv.DictWriter(f_manifest, fieldnames=file_output_fields, delimiter=CSV_SEP)
            manifest_writer.writeheader()

            def flush_manifest_buffer() -> None:
                if not row_buffer:
                    return
                manifest_writer.writerows(row_buffer)
                row_buffer.clear()
                f_manifest.flush()

            while dir_stack:
                if self.cancel_event.is_set():
                    flush_manifest_buffer()
                    write_telemetry_event(
                        events,
                        run,
                        "ANALYSIS_CANCELLED",
                        "Analise cancelada pelo usuario.",
                        f"files_scanned={total_files};dirs_processed={dirs_processed}",
                    )
                    raise WorkflowCancelled("Analise cancelada pelo usuario.")

                folder = dir_stack.pop()
                dirs_processed += 1
                folder_key = str(folder)
                try:
                    with os.scandir(folder) as it:
                        for entry in it:
                            if entry.is_dir(follow_symlinks=False):
                                dir_stack.append(Path(entry.path))
                                dirs_discovered += 1
                                continue
                            if not entry.is_file(follow_symlinks=False):
                                continue

                            seq += 1
                            try:
                                size_actual = entry.stat(follow_symlinks=False).st_size
                            except Exception:
                                size_actual = 0
                            # Always keep aggregate totals meaningful, even when per-file size collection is disabled.
                            size = size_actual if self.cfg.collect_size_bytes else 0

                            ext = Path(entry.name).suffix.lower()
                            no_ext = ext == ""
                            if restrict_extensions:
                                include = (ext in allowed_ext) or (no_ext and include_no_ext)
                                reason = (
                                    "INCLUDED_EXT"
                                    if ext in allowed_ext
                                    else ("INCLUDED_NO_EXT" if (no_ext and include_no_ext) else "EXCLUDED_EXTENSION")
                                )
                            else:
                                include = True
                                reason = "INCLUDED_ALL_FILES"
                            if include:
                                selected_files += 1
                                selected_bytes += size_actual
                                selected_folder_keys.add(folder_key)
                            else:
                                excluded_files += 1

                            total_files += 1
                            total_bytes += size_actual
                            agg = folder_agg.setdefault(folder_key, {"count": 0, "bytes": 0})
                            agg["count"] += 1
                            agg["bytes"] += size_actual

                            ts_br, ts_iso = now_dual_timestamp()
                            row_buffer.append(
                                {
                                    "run_id": run,
                                    "seq": seq,
                                    "file_path": entry.path,
                                    "folder_path": folder_key,
                                    "extension": ext,
                                    "size_bytes": size,
                                    "selected_for_send": 1 if include else 0,
                                    "selection_reason": reason,
                                    "dicom_status": "UNKNOWN",
                                    "discovered_at": ts_br,
                                    "timestamp_br": ts_br,
                                    "timestamp_iso": ts_iso,
                                }
                            )
                            if len(row_buffer) >= buffer_size:
                                flush_manifest_buffer()
                except Exception as ex:
                    scan_errors += 1
                    if scan_errors <= 5:
                        self._log(f"[WARN] Falha ao escanear pasta: {folder} | erro={ex}")

                now_ts = time.monotonic()
                if (now_ts - last_progress_ts) >= progress_interval_sec:
                    flush_manifest_buffer()
                    elapsed = max(now_ts - start_ts, 0.001)
                    rate_files = total_files / elapsed
                    avg_files_per_dir = total_files / max(dirs_processed, 1)
                    est_total_files = total_files + int(len(dir_stack) * avg_files_per_dir)
                    remaining_files = max(est_total_files - total_files, 0)
                    eta_seconds = (remaining_files / rate_files) if rate_files > 0 else None
                    self._log(
                        f"[AN_SCAN_PROGRESS] dirs={dirs_processed} pending_dirs={len(dir_stack)} "
                        f"files={total_files} selected={selected_files} rate={rate_files:.1f} arq/s "
                        f"eta~{format_eta(eta_seconds)}"
                    )
                    self._progress(
                        f"progresso analise: dirs={dirs_processed} pendentes={len(dir_stack)} "
                        f"arquivos={total_files} selecionados={selected_files} "
                        f"taxa={rate_files:.1f} arq/s eta~{format_eta(eta_seconds)}"
                    )
                    last_progress_ts = now_ts

            flush_manifest_buffer()

        folder_fields = ["run_id", "folder_path", "file_count", "size_bytes", "discovered_at"]
        for folder, agg in sorted(folder_agg.items()):
            write_csv_row(
                manifest_folders,
                {
                    "run_id": run,
                    "folder_path": folder,
                    "file_count": agg["count"],
                    "size_bytes": agg["bytes"],
                    "discovered_at": now_br(),
                },
                folder_fields,
            )

        dcm4che_send_mode = normalize_dcm4che_send_mode(self.cfg.dcm4che_send_mode)
        use_folder_unit = self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS"
        chunk_unit = "pastas" if use_folder_unit else "arquivos"
        selected_folder_count = len(selected_folder_keys)
        chunk_base_count = selected_folder_count if use_folder_unit else selected_files
        chunk_total = math.ceil(chunk_base_count / batch_size) if chunk_base_count else 0
        summary_fields = [
            "run_id",
            "root_path",
            "toolkit",
            "batch_size",
            "folders_total",
            "folders_selected_for_send",
            "files_total",
            "files_selected_for_send",
            "files_excluded",
            "size_total_bytes",
            "size_selected_bytes",
            "size_collection_enabled",
            "chunk_unit",
            "chunks_total",
            "generated_at",
        ]
        write_csv_row(
            summary,
            {
                "run_id": run,
                "root_path": str(root),
                "toolkit": self.cfg.toolkit,
                "batch_size": batch_size,
                "folders_total": len(folder_agg),
                "folders_selected_for_send": selected_folder_count,
                "files_total": total_files,
                "files_selected_for_send": selected_files,
                "files_excluded": excluded_files,
                "size_total_bytes": total_bytes,
                "size_selected_bytes": selected_bytes,
                "size_collection_enabled": "1" if self.cfg.collect_size_bytes else "0",
                "chunk_unit": chunk_unit,
                "chunks_total": chunk_total,
                "generated_at": now_br(),
            },
            summary_fields,
        )
        write_telemetry_event(
            events,
            run,
            "ANALYSIS_END",
            "Analise concluida.",
            (
                f"files_total={total_files};selected_files={selected_files};selected_folders={selected_folder_count};"
                f"chunks={chunk_total};chunk_unit={chunk_unit};scan_errors={scan_errors};"
                f"collect_size_bytes={'1' if self.cfg.collect_size_bytes else '0'}"
            ),
        )

        self._log(
            f"[AN_RESULT] arquivos={total_files} selecionados={selected_files} "
            f"pastas_selecionadas={selected_folder_count} chunks={chunk_total} ({chunk_unit})"
        )
        self._log(f"[AN_END] run_id={run} status=PASS")
        self._progress(
            f"progresso analise: concluido | arquivos={total_files} selecionados={selected_files} "
            f"chunks={chunk_total}"
        )
        return {
            "run_id": run,
            "run_dir": str(run_dir),
            "chunks_total": chunk_total,
            "chunk_unit": chunk_unit,
            "files_total": total_files,
            "files_selected": selected_files,
            "folders_total": len(folder_agg),
            "folders_selected": selected_folder_count,
            "size_total_bytes": total_bytes,
            "size_selected_bytes": selected_bytes,
        }


class SendWorkflow:
    def __init__(self, cfg: AppConfig, logger, cancel_event: threading.Event, progress_callback, toolkit_logger=None):
        self.cfg = cfg
        self.logger = logger
        self.toolkit_logger = toolkit_logger or logger
        self.cancel_event = cancel_event
        self.progress_callback = progress_callback
        self.current_proc: subprocess.Popen | None = None
        apply_internal_toolkit_paths(self.cfg, Path(__file__).resolve().parent, self._log)
        self.driver = get_driver(cfg.toolkit)

    def _log(self, msg: str) -> None:
        self.logger(msg)

    def _log_toolkit(self, msg: str) -> None:
        self.toolkit_logger(msg)

    def _resolve_runs_base(self, script_dir: Path) -> Path:
        if self.cfg.runs_base_dir.strip():
            p = Path(self.cfg.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (script_dir / p).resolve()
        return (script_dir / "runs").resolve()

    def _kill_current_process_tree(self) -> None:
        if self.current_proc is None or self.current_proc.poll() is not None:
            return
        pid = self.current_proc.pid
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True,
                text=True,
                check=False,
                **hidden_process_kwargs(),
            )
        except Exception:
            pass

    def run_send(self, run_id: str, batch_size: int, show_output: bool = True) -> dict:
        script_dir = Path(__file__).resolve().parent
        runs_base = self._resolve_runs_base(script_dir)
        run = run_id.strip()
        if not run:
            raise RuntimeError("run_id e obrigatorio para envio.")
        run_dir = runs_base / run
        if not run_dir.exists():
            raise RuntimeError(f"Run nao encontrado: {run_dir}")
        if batch_size < 1:
            raise RuntimeError("batch_size deve ser >= 1")

        ts_mode = (self.cfg.ts_mode or "AUTO").upper().strip()
        if ts_mode != "AUTO":
            self._log(f"[WARN] TS mode '{ts_mode}' ainda nao implementado. Usando AUTO.")
            ts_mode = "AUTO"
        if self.cfg.toolkit == "dcm4che" and not self.cfg.dcm4che_use_shell_wrapper:
            self._log("[WARN] dcm4che sem wrapper de shell ativo (modo experimental).")
        dcm4che_send_mode = normalize_dcm4che_send_mode(self.cfg.dcm4che_send_mode)
        self._log(
            f"[SEND_CONFIG] toolkit={self.cfg.toolkit} "
            f"dcm4che_send_mode={dcm4che_send_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'}"
        )
        self._log("[RUN_LAYOUT] mode=send layout=core|telemetry|reports")

        manifest_files = resolve_run_artifact_path(run_dir, "manifest_files.csv", for_write=False, logger=self._log)
        if not manifest_files.exists():
            raise RuntimeError(f"Arquivo nao encontrado: {manifest_files}")
        rows = read_csv_rows(manifest_files)
        selected_rows = [r for r in rows if str(r.get("selected_for_send", "0")).strip() == "1"]
        selected = [Path(r["file_path"]) for r in selected_rows]
        total_items = len(selected)
        if total_items == 0:
            raise RuntimeError("Nenhum arquivo selecionado no manifesto para envio.")
        folder_to_files: dict[str, list[Path]] = {}
        for r in selected_rows:
            folder = str(r.get("folder_path", "")).strip() or str(Path(r["file_path"]).parent)
            folder_to_files.setdefault(folder, []).append(Path(r["file_path"]))

        checkpoint_name = send_checkpoint_filename(self.cfg)
        checkpoint_read = resolve_run_artifact_path(run_dir, checkpoint_name, for_write=False, logger=self._log)

        done_units = 0
        done_files = 0
        if checkpoint_read.exists():
            try:
                payload = json.loads(checkpoint_read.read_text(encoding="utf-8"))
                done_units = int(payload.get("done_units", payload.get("done_items", 0)))
                done_files = int(payload.get("done_files", payload.get("done_items", 0)))
            except Exception:
                done_units = 0
                done_files = 0

        if done_units == 0:
            for filename in [
                "storescu_execucao.log",
                "send_results_by_file.csv",
                "send_summary.csv",
            ]:
                cleanup_run_artifact_variants(run_dir, filename)
            for legacy_name in ["analysis_events.csv", "send_events.csv", "send_errors.csv", "consistency_events.csv"]:
                cleanup_run_artifact_variants(run_dir, legacy_name)
            self._log(f"RUN_ID envio: {run}")

        log_file = resolve_run_artifact_path(run_dir, "storescu_execucao.log", for_write=True, logger=self._log)
        events = resolve_run_artifact_path(run_dir, "events.csv", for_write=True, logger=self._log)
        send_results = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=True, logger=self._log)
        send_summary = resolve_run_artifact_path(run_dir, "send_summary.csv", for_write=True, logger=self._log)
        checkpoint = resolve_run_artifact_path(run_dir, checkpoint_name, for_write=True, logger=self._log)
        args_dir = resolve_run_batch_args_dir(run_dir, for_write=True, logger=self._log)

        if self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS":
            manifest_folders = resolve_run_artifact_path(run_dir, "manifest_folders.csv", for_write=False, logger=self._log)
            folder_keys = set(folder_to_files.keys())
            ordered_folders: list[str] = []
            if manifest_folders.exists():
                for fr in read_csv_rows(manifest_folders):
                    fp = str(fr.get("folder_path", "")).strip()
                    if fp in folder_keys:
                        ordered_folders.append(fp)
            else:
                ordered_folders = sorted(folder_keys)
            units_total = len(ordered_folders)
            chunks = [ordered_folders[i : i + batch_size] for i in range(done_units, units_total, batch_size)]
            total_chunks = math.ceil(units_total / batch_size) if units_total else 0
        else:
            units_total = total_items
            chunks = [selected[i : i + batch_size] for i in range(done_units, units_total, batch_size)]
            total_chunks = math.ceil(units_total / batch_size) if units_total else 0

        if units_total > 0 and done_units >= units_total:
            prev_status = ""
            if send_summary.exists():
                prev_rows = read_csv_rows(send_summary)
                if prev_rows:
                    prev_status = str(prev_rows[-1].get("status", "")).strip()
            if prev_status == "PASS":
                msg = "Este run ja foi enviado com sucesso anteriormente. Nenhum item pendente para envio."
                status = "ALREADY_SENT_PASS"
            else:
                msg = "Este run nao possui itens pendentes para envio."
                status = "ALREADY_SENT"
            self._log(msg)
            write_telemetry_event(events, run, "RUN_SEND_SKIP_ALREADY_COMPLETED", msg, f"prev_status={prev_status or 'N/A'}")
            return {"run_id": run, "status": status, "run_dir": str(run_dir)}

        result_fields = [
            "run_id",
            "file_path",
            "chunk_no",
            "toolkit",
            "ts_mode",
            "send_status",
            "status_detail",
            "sop_instance_uid",
            "source_ts_uid",
            "source_ts_name",
            "extract_status",
            "processed_at",
        ]

        write_telemetry_event(
            events,
            run,
            "RUN_SEND_START",
            "Envio iniciado.",
            (
                f"total_items={total_items};batch={batch_size};toolkit={self.cfg.toolkit};"
                f"dcm4che_send_mode={dcm4che_send_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'}"
            ),
        )
        self._log(
            f"[SEND_START] total_items={total_items} batch={batch_size} "
            f"toolkit={self.cfg.toolkit} mode={dcm4che_send_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'}"
        )

        sent_ok = 0
        warned = 0
        failed = 0
        warn_type_counts: dict[str, int] = {
            "SENT_UNKNOWN": 0,
            "NON_DICOM": 0,
            "UNSUPPORTED_DICOM_OBJECT": 0,
            "UID_EMPTY_EXPECTED": 0,
            "UID_EMPTY_UNEXPECTED": 0,
            "PARSE_EXCEPTION": 0,
        }
        interrupted = False
        item_cursor = done_files
        unit_cursor = done_units

        for chunk_index, batch in enumerate(chunks, start=(done_units // batch_size) + 1):
            if self.cancel_event.is_set():
                interrupted = True
                break
            if self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS":
                batch_inputs = [Path(x) for x in batch]
                batch_files: list[Path] = []
                for folder in batch:
                    batch_files.extend(folder_to_files.get(str(folder), []))
            else:
                batch_inputs = list(batch)
                batch_files = list(batch)
            first_item = item_cursor + 1
            last_item = min(item_cursor + len(batch_files), total_items)
            self.progress_callback(first_item, total_items, chunk_index, total_chunks)
            self._log(
                f"[CHUNK_START] chunk={chunk_index}/{total_chunks} "
                f"itens={first_item}-{last_item}/{total_items} "
                f"units={len(batch_inputs)} files={len(batch_files)}"
            )
            write_telemetry_event(
                events,
                run,
                "CHUNK_START",
                "Chunk iniciado.",
                f"chunk_no={chunk_index};items={len(batch_files)};units={len(batch_inputs)}",
            )

            args_file = args_dir / f"batch_{chunk_index:06d}.txt"
            with args_file.open("w", encoding="utf-8") as f:
                for file_path in batch_files:
                    f.write(f"\"{file_path}\"\n")

            cmd = self.driver.storescu_cmd(self.cfg, batch_inputs, args_file)
            lines: list[str] = []
            exit_code = -1
            with log_file.open("a", encoding="utf-8", errors="replace") as lf:
                self.current_proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    **hidden_process_kwargs(),
                )
                try:
                    assert self.current_proc.stdout is not None
                    for line in self.current_proc.stdout:
                        if self.cancel_event.is_set():
                            self._kill_current_process_tree()
                            interrupted = True
                            break
                        clean = line.rstrip("\n")
                        lines.append(clean)
                        lf.write(line)
                        lf.flush()
                        if show_output:
                            self._log_toolkit(clean)
                    if not interrupted:
                        self.current_proc.wait()
                        exit_code = self.current_proc.returncode if self.current_proc.returncode is not None else -1
                finally:
                    self.current_proc = None
            if interrupted:
                break

            parse_exception_by_file: dict[str, list[str]] = {}
            current_scan_file = ""
            for ln in lines:
                m_scan = re.search(r"Failed to scan file (.+?):\s*(.+)$", ln)
                if m_scan:
                    current_scan_file = m_scan.group(1).strip()
                    reason = m_scan.group(2).strip()
                    parse_exception_by_file.setdefault(current_scan_file, []).append(reason)
                    continue
                if current_scan_file and (
                    "DicomStreamException" in ln
                    or "IllegalArgumentException" in ln
                    or "EOFException" in ln
                    or "Unrecognized VR code" in ln
                ):
                    parse_exception_by_file.setdefault(current_scan_file, []).append(ln.strip())

            parsed = self.driver.parse_send_output(lines, batch_inputs)
            if self.cfg.toolkit == "dcm4che":
                batch_info = parsed.get("__batch__", {})
                rq_iuid_list = [sanitize_uid(x) for x in batch_info.get("rq_iuids", []) if sanitize_uid(x)]
                rq_iuid_set = set(rq_iuid_list)
                ok_iuids = list(batch_info.get("ok_iuids", []))
                err_iuids = list(batch_info.get("err_iuids", []))
                ok_iuid_set = set(ok_iuids)
                err_iuid_set = set(err_iuids)
                err_status_by_iuid = dict(batch_info.get("err_status_by_iuid", {}))

                # Deterministic fallback: align request IUID sequence with likely DICOM payload files.
                inferred_iuid_by_file: dict[str, str] = {}
                rq_cursor = 0
                for candidate in batch_files:
                    cfp = str(candidate)
                    if not looks_like_dicom_payload_file(candidate):
                        continue
                    if rq_cursor >= len(rq_iuid_list):
                        break
                    inferred_iuid_by_file[cfp] = rq_iuid_list[rq_cursor]
                    rq_cursor += 1

                for file_path in batch_files:
                    fp = str(file_path)
                    item_cursor += 1
                    src_iuid = ""
                    src_ts_uid = ""
                    src_ts_name = ""
                    extract_status = ""
                    meta_err = ""
                    try:
                        src_iuid, src_ts_uid, src_ts_name, meta_err = self.driver.extract_metadata(self.cfg, file_path)
                    except Exception as ex:
                        meta_err = str(ex)
                    src_iuid = sanitize_uid(src_iuid)
                    src_ts_uid = sanitize_uid(src_ts_uid)
                    src_ts_name = sanitize_uid(src_ts_name)

                    # Fallback: many datasets already embed SOPInstanceUID in filename.
                    if not src_iuid:
                        src_iuid = sanitize_uid(Path(fp).name)
                    inferred_iuid = inferred_iuid_by_file.get(fp, "")
                    if (
                        inferred_iuid
                        and (
                            (not src_iuid)
                            or (src_iuid not in ok_iuid_set and src_iuid not in err_iuid_set and src_iuid not in rq_iuid_set)
                        )
                    ):
                        if src_iuid and src_iuid != inferred_iuid:
                            src_iuid_prev = src_iuid
                            src_iuid = inferred_iuid
                        else:
                            src_iuid_prev = ""
                            src_iuid = inferred_iuid
                        uid_was_inferred = True
                    else:
                        src_iuid_prev = ""
                        uid_was_inferred = False

                    detail = (
                        f"dcm4che parse: rq_iuids={len(rq_iuid_set)};ok_iuids={len(ok_iuids)};"
                        f"err_iuids={len(err_iuids)};exit_code={exit_code}"
                    )
                    if meta_err:
                        detail += f";meta_err={meta_err}"
                    if src_iuid_prev:
                        detail += f";uid_override={src_iuid_prev}->{src_iuid}"
                    elif uid_was_inferred:
                        detail += ";uid_inferred=RQ_ORDER"
                    if not src_iuid:
                        detail += ";uid_extract=EMPTY"

                    if src_iuid and src_iuid in ok_iuid_set:
                        status = "SENT_OK"
                        extract_status = "OK_FROM_STORESCU"
                    elif src_iuid and src_iuid in err_iuid_set:
                        status = "SEND_FAIL"
                        detail += f";rsp_status={err_status_by_iuid.get(src_iuid, 'UNKNOWN')}"
                        extract_status = "ERR_FROM_STORESCU"
                    elif src_iuid and src_iuid in rq_iuid_set:
                        # Request sent but no explicit success/error response in parsed output.
                        status = "SENT_UNKNOWN"
                        extract_status = "REQUESTED_NO_RSP"
                    elif exit_code != 0:
                        status = "SEND_FAIL"
                        extract_status = "PROCESS_EXIT_FAIL"
                    else:
                        status = "SENT_UNKNOWN"
                        extract_status = "NO_MATCH"

                    if status == "SENT_OK":
                        sent_ok += 1
                    elif status in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"]:
                        warned += 1
                        warn_type_counts[status] = warn_type_counts.get(status, 0) + 1
                    else:
                        failed += 1

                    write_csv_row(
                        send_results,
                        {
                            "run_id": run,
                            "file_path": fp,
                            "chunk_no": chunk_index,
                            "toolkit": self.cfg.toolkit,
                            "ts_mode": ts_mode,
                            "send_status": status,
                            "status_detail": detail,
                            "sop_instance_uid": src_iuid,
                            "source_ts_uid": src_ts_uid,
                            "source_ts_name": src_ts_name,
                            "extract_status": extract_status,
                            "processed_at": now_br(),
                        },
                        result_fields,
                    )
                    if status != "SENT_OK":
                        write_telemetry_event(
                            events,
                            run,
                            "SEND_FILE_ERROR",
                            detail or status,
                            f"chunk_no={chunk_index};file_path={fp};error_type={status}",
                        )
                    if src_iuid and (status in ["SENT_UNKNOWN", "SEND_FAIL"]) and (src_iuid not in ok_iuid_set):
                        self._log(
                            f"[SEND_PARSE_MISMATCH] file={fp} iuid={src_iuid} "
                            f"mode={dcm4che_send_mode} status={status} extract_status={extract_status}"
                        )
                    elif not src_iuid:
                        if Path(fp).name.upper() == "DICOMDIR":
                            warn_type_counts["UID_EMPTY_EXPECTED"] = warn_type_counts.get("UID_EMPTY_EXPECTED", 0) + 1
                            self._log(
                                f"[SEND_PARSE_UID_EMPTY_EXPECTED] file={fp} mode={dcm4che_send_mode} "
                                f"status={status} extract_status={extract_status}"
                            )
                        else:
                            warn_type_counts["UID_EMPTY_UNEXPECTED"] = warn_type_counts.get("UID_EMPTY_UNEXPECTED", 0) + 1
                            self._log(
                                f"[SEND_PARSE_UID_EMPTY] file={fp} mode={dcm4che_send_mode} "
                                f"status={status} extract_status={extract_status}"
                            )
                    parse_notes = parse_exception_by_file.get(fp, [])
                    if parse_notes:
                        warn_type_counts["PARSE_EXCEPTION"] = warn_type_counts.get("PARSE_EXCEPTION", 0) + 1
                        write_telemetry_event(
                            events,
                            run,
                            "SEND_PARSE_EXCEPTION",
                            parse_notes[0],
                            f"chunk_no={chunk_index};file_path={fp};errors={len(parse_notes)}",
                        )
                    self.progress_callback(item_cursor, total_items, chunk_index, total_chunks)
            else:
                for file_path in batch_files:
                    fp = str(file_path)
                    item_cursor += 1
                    base = parsed.get(fp, {"send_status": "SENT_UNKNOWN", "status_detail": ""})
                    status = base.get("send_status", "SENT_UNKNOWN")
                    detail = base.get("status_detail", "")
                    iuid = ""
                    ts_uid = ""
                    ts_name = ""
                    extract_status = ""

                    miuid, mts_uid, mts_name, m_err = self.driver.extract_metadata(self.cfg, file_path)
                    iuid = miuid
                    ts_uid = mts_uid
                    ts_name = mts_name
                    if iuid:
                        extract_status = "OK"
                    elif status == "SENT_OK":
                        extract_status = "MISSING_IUID"
                    if m_err and status == "SENT_OK":
                        detail = (detail + " | " + m_err).strip(" |")

                    if status == "SENT_OK":
                        sent_ok += 1
                    elif status in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"]:
                        warned += 1
                        warn_type_counts[status] = warn_type_counts.get(status, 0) + 1
                    else:
                        failed += 1

                    write_csv_row(
                        send_results,
                        {
                            "run_id": run,
                            "file_path": fp,
                            "chunk_no": chunk_index,
                            "toolkit": self.cfg.toolkit,
                            "ts_mode": ts_mode,
                            "send_status": status,
                            "status_detail": detail,
                            "sop_instance_uid": iuid,
                            "source_ts_uid": ts_uid,
                            "source_ts_name": ts_name,
                            "extract_status": extract_status,
                            "processed_at": now_br(),
                        },
                        result_fields,
                    )
                    if status != "SENT_OK":
                        write_telemetry_event(
                            events,
                            run,
                            "SEND_FILE_ERROR",
                            detail or status,
                            f"chunk_no={chunk_index};file_path={fp};error_type={status}",
                        )
                    parse_notes = parse_exception_by_file.get(fp, [])
                    if parse_notes:
                        warn_type_counts["PARSE_EXCEPTION"] = warn_type_counts.get("PARSE_EXCEPTION", 0) + 1
                        write_telemetry_event(
                            events,
                            run,
                            "SEND_PARSE_EXCEPTION",
                            parse_notes[0],
                            f"chunk_no={chunk_index};file_path={fp};errors={len(parse_notes)}",
                        )
                    self.progress_callback(item_cursor, total_items, chunk_index, total_chunks)
            unit_cursor += len(batch_inputs)
            checkpoint.write_text(
                json.dumps(
                    {"run_id": run, "done_units": unit_cursor, "done_files": item_cursor, "updated_at": now_br()},
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            write_telemetry_event(
                events,
                run,
                "CHUNK_END",
                "Chunk concluido.",
                f"chunk_no={chunk_index};exit_code={exit_code}",
            )
            self._log(
                f"[CHUNK_END] chunk={chunk_index}/{total_chunks} exit_code={exit_code} "
                f"processed_items={item_cursor}/{total_items}"
            )

        final_status = "INTERRUPTED" if interrupted else ("PASS" if failed == 0 and warned == 0 else ("PASS_WITH_WARNINGS" if failed == 0 else "FAIL"))
        write_csv_row(
            send_summary,
            {
                "run_id": run,
                "toolkit": self.cfg.toolkit,
                "ts_mode_effective": ts_mode,
                "total_items": total_items,
                "items_processed": item_cursor,
                "sent_ok": sent_ok,
                "warnings": warned,
                "failed": failed,
                "status": final_status,
                "finished_at": now_br(),
            },
            ["run_id", "toolkit", "ts_mode_effective", "total_items", "items_processed", "sent_ok", "warnings", "failed", "status", "finished_at"],
        )
        write_telemetry_event(events, run, "RUN_SEND_END", "Envio finalizado.", f"status={final_status}")
        self._log(f"[SEND_END] status={final_status} processed_items={item_cursor}/{total_items}")
        self._log(f"[SEND_RESULT] ok={sent_ok} warn={warned} fail={failed} status={final_status}")
        if warned > 0:
            self._log(
                "[SEND_WARN_SUMMARY] "
                f"sent_unknown={warn_type_counts.get('SENT_UNKNOWN', 0)} "
                f"non_dicom={warn_type_counts.get('NON_DICOM', 0)} "
                f"unsupported={warn_type_counts.get('UNSUPPORTED_DICOM_OBJECT', 0)} "
                f"uid_empty_expected={warn_type_counts.get('UID_EMPTY_EXPECTED', 0)} "
                f"uid_empty_unexpected={warn_type_counts.get('UID_EMPTY_UNEXPECTED', 0)} "
                f"parse_exception_files={warn_type_counts.get('PARSE_EXCEPTION', 0)}"
            )
        return {"run_id": run, "status": final_status, "run_dir": str(run_dir)}


class ValidationWorkflow:
    def __init__(self, cfg: AppConfig, logger, cancel_event: threading.Event):
        self.cfg = cfg
        self.logger = logger
        self.cancel_event = cancel_event
        apply_internal_toolkit_paths(self.cfg, Path(__file__).resolve().parent, self._log)
        self.driver = get_driver(cfg.toolkit)

    def _log(self, msg: str) -> None:
        self.logger(msg)

    def _resolve_runs_base(self, script_dir: Path) -> Path:
        if self.cfg.runs_base_dir.strip():
            p = Path(self.cfg.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (script_dir / p).resolve()
        return (script_dir / "runs").resolve()

    def _query_instance_dataset(self, iuid: str) -> dict:
        url = f"http://{self.cfg.pacs_rest_host}/dcm4chee-arc/aets/{self.cfg.aet_destino}/rs/instances?SOPInstanceUID={iuid}"
        api_found = 0
        http_status = ""
        detail = ""
        dataset: dict = {}
        try:
            req = urlrequest.Request(url, method="GET")
            with urlrequest.urlopen(req, timeout=20) as resp:
                http_status = str(resp.status)
                body = resp.read().decode("utf-8", errors="replace")
                data = json.loads(body) if body.strip() else []
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    api_found = 1
                    dataset = data[0]
        except urlerror.HTTPError as ex:
            http_status = str(ex.code)
            detail = str(ex)
        except Exception as ex:
            http_status = "ERR"
            detail = str(ex)
        return {
            "api_found": api_found,
            "http_status": http_status,
            "detail": detail,
            "dataset": dataset,
        }

    def _dicom_text(self, dataset: dict, tag: str) -> str:
        elem = dataset.get(tag, {})
        if not isinstance(elem, dict):
            return ""
        values = elem.get("Value", [])
        if not isinstance(values, list) or not values:
            return ""
        first = values[0]
        if isinstance(first, dict):
            if "Alphabetic" in first:
                return str(first.get("Alphabetic", "")).strip()
            for v in first.values():
                if v is not None:
                    return str(v).strip()
            return ""
        return str(first).strip()

    def _report_fields_from_dataset(self, dataset: dict) -> dict:
        return {
            "nome_paciente": self._dicom_text(dataset, "00100010"),
            "data_nascimento": self._dicom_text(dataset, "00100030"),
            "prontuario": self._dicom_text(dataset, "00100020"),
            "accession_number": self._dicom_text(dataset, "00080050"),
            "sexo": self._dicom_text(dataset, "00100040"),
            "data_exame": self._dicom_text(dataset, "00080020"),
            "descricao_exame": self._dicom_text(dataset, "00081030"),
            "study_uid": self._dicom_text(dataset, "0020000D"),
        }

    def export_complete_report(self, run_id: str, report_mode: str = "A") -> dict:
        run = run_id.strip()
        if not run:
            raise RuntimeError("run_id e obrigatorio para exportar relatorio.")
        mode = (report_mode or "A").strip().upper()
        if mode not in ["A", "C"]:
            raise RuntimeError(f"Modo de relatorio invalido: {report_mode}")

        script_dir = Path(__file__).resolve().parent
        run_dir = self._resolve_runs_base(script_dir) / run
        if not run_dir.exists():
            raise RuntimeError(f"Run nao encontrado: {run_dir}")
        self._log("[RUN_LAYOUT] mode=report_export layout=core|telemetry|reports")

        send_results = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=True, logger=self._log)
        legacy_file_iuid_map = resolve_run_artifact_path(run_dir, "file_iuid_map.csv", for_write=False, logger=self._log)
        if not send_results.exists():
            raise RuntimeError(f"Arquivo nao encontrado: {send_results}")

        send_rows = read_csv_rows(send_results)
        map_by_file = build_iuid_map_from_send_rows(send_rows)
        merge_iuid_map_from_legacy_file(map_by_file, legacy_file_iuid_map)

        sent_ok_rows = [r for r in send_rows if r.get("send_status", "") == "SENT_OK"]
        if not sent_ok_rows:
            raise RuntimeError("Nenhum arquivo SENT_OK encontrado para exportacao.")

        report_records: list[dict] = []
        updates_by_file: dict[str, dict] = {}
        for row in sent_ok_rows:
            fp = row.get("file_path", "").strip()
            if not fp:
                continue
            meta = map_by_file.get(fp, {})
            iuid = str(meta.get("sop_instance_uid", "")).strip()
            if not iuid:
                iuid, ts_uid, ts_name, err = self.driver.extract_metadata(self.cfg, Path(fp))
                if iuid:
                    map_by_file[fp] = {
                        "sop_instance_uid": iuid,
                        "source_ts_uid": ts_uid,
                        "source_ts_name": ts_name,
                        "extract_status": "REPORT_EXPORT_OK",
                    }
                    updates_by_file[fp] = {
                        "sop_instance_uid": iuid,
                        "source_ts_uid": ts_uid,
                        "source_ts_name": ts_name,
                        "extract_status": "REPORT_EXPORT_OK",
                    }
                else:
                    self._log(f"[WARN] IUID ausente para arquivo no relatorio: {fp} | erro={err or 'desconhecido'}")
            report_records.append({"file_path": fp, "sop_instance_uid": iuid})

        updated_rows = apply_send_result_updates(send_results, run, updates_by_file)
        if updated_rows > 0:
            self._log(f"[CORE_COMPACT] send_results_by_file atualizado com IUID para {updated_rows} arquivo(s).")

        unique_iuids = sorted({r["sop_instance_uid"] for r in report_records if r["sop_instance_uid"]})
        self._log(f"[REPORT_EXPORT] Modo {mode} | IUIDs unicos para consulta: {len(unique_iuids)}")

        iuid_data: dict[str, dict] = {}
        done = 0
        for iuid in unique_iuids:
            if self.cancel_event.is_set():
                raise RuntimeError("Exportacao de relatorio cancelada.")
            query = self._query_instance_dataset(iuid)
            fields = self._report_fields_from_dataset(query.get("dataset", {}))
            status = "OK" if query.get("api_found", 0) == 1 else "ERRO"
            iuid_data[iuid] = {
                **fields,
                "status": status,
                "http_status": str(query.get("http_status", "")),
                "detail": str(query.get("detail", "")),
            }
            done += 1
            if done % 100 == 0:
                self._log(f"[REPORT_EXPORT_PROGRESS] {done}/{len(unique_iuids)} IUIDs consultados")

        rows_a: list[dict] = []
        for rec in report_records:
            fp = rec.get("file_path", "")
            iuid = rec.get("sop_instance_uid", "")
            base = iuid_data.get(
                iuid,
                {
                    "nome_paciente": "",
                    "data_nascimento": "",
                    "prontuario": "",
                    "accession_number": "",
                    "sexo": "",
                    "data_exame": "",
                    "descricao_exame": "",
                    "study_uid": "",
                    "status": "ERRO",
                    "http_status": "",
                    "detail": "IUID ausente",
                },
            )
            rows_a.append(
                {
                    "run_id": run,
                    "file_path": fp,
                    "sop_instance_uid": iuid,
                    "nome_paciente": base.get("nome_paciente", ""),
                    "data_nascimento": base.get("data_nascimento", ""),
                    "prontuario": base.get("prontuario", ""),
                    "accession_number": base.get("accession_number", ""),
                    "sexo": base.get("sexo", ""),
                    "data_exame": base.get("data_exame", ""),
                    "descricao_exame": base.get("descricao_exame", ""),
                    "study_uid": base.get("study_uid", ""),
                    "status": base.get("status", "ERRO"),
                }
            )

        if mode == "A":
            report_file = resolve_run_artifact_path(
                run_dir, "validation_full_report_A.csv", for_write=True, logger=self._log, keep_legacy_on_write=False
            )
            fieldnames = [
                "run_id",
                "file_path",
                "sop_instance_uid",
                "nome_paciente",
                "data_nascimento",
                "prontuario",
                "accession_number",
                "sexo",
                "data_exame",
                "descricao_exame",
                "study_uid",
                "status",
            ]
            write_csv_table(report_file, rows_a, fieldnames)
            status_ok = sum(1 for r in rows_a if r.get("status") == "OK")
            status_err = len(rows_a) - status_ok
            self._log(f"[REPORT_EXPORT] Relatorio A exportado: {report_file} | linhas={len(rows_a)} ok={status_ok} erro={status_err}")
            return {"run_id": run, "mode": mode, "report_file": str(report_file), "rows": len(rows_a), "ok": status_ok, "erro": status_err}

        grouped: dict[str, dict] = {}
        for row in rows_a:
            study_uid = row.get("study_uid", "").strip()
            key = study_uid if study_uid else f"__ERRO__{row.get('sop_instance_uid', '').strip() or row.get('file_path', '').strip()}"
            agg = grouped.setdefault(
                key,
                {
                    "run_id": run,
                    "study_uid": study_uid,
                    "nome_paciente": "",
                    "data_nascimento": "",
                    "prontuario": "",
                    "accession_number": "",
                    "sexo": "",
                    "data_exame": "",
                    "descricao_exame": "",
                    "status": "OK",
                    "total_arquivos": 0,
                },
            )
            agg["total_arquivos"] = int(agg.get("total_arquivos", 0)) + 1
            for f in ["nome_paciente", "data_nascimento", "prontuario", "accession_number", "sexo", "data_exame", "descricao_exame"]:
                if not agg.get(f):
                    agg[f] = row.get(f, "")
            if not agg.get("study_uid"):
                agg["study_uid"] = study_uid
            if row.get("status", "ERRO") == "ERRO":
                agg["status"] = "ERRO"

        rows_c = sorted(grouped.values(), key=lambda x: str(x.get("study_uid", "")))
        report_file = resolve_run_artifact_path(
            run_dir, "validation_full_report_C.csv", for_write=True, logger=self._log, keep_legacy_on_write=False
        )
        fieldnames = [
            "run_id",
            "study_uid",
            "nome_paciente",
            "data_nascimento",
            "prontuario",
            "accession_number",
            "sexo",
            "data_exame",
            "descricao_exame",
            "status",
            "total_arquivos",
        ]
        write_csv_table(report_file, rows_c, fieldnames)
        status_ok = sum(1 for r in rows_c if r.get("status") == "OK")
        status_err = len(rows_c) - status_ok
        self._log(f"[REPORT_EXPORT] Relatorio C exportado: {report_file} | linhas={len(rows_c)} ok={status_ok} erro={status_err}")
        return {"run_id": run, "mode": mode, "report_file": str(report_file), "rows": len(rows_c), "ok": status_ok, "erro": status_err}

    def run_validation(self, run_id: str) -> dict:
        run = run_id.strip()
        if not run:
            raise RuntimeError("run_id e obrigatorio para validacao.")
        script_dir = Path(__file__).resolve().parent
        run_dir = self._resolve_runs_base(script_dir) / run
        if not run_dir.exists():
            raise RuntimeError(f"Run nao encontrado: {run_dir}")
        self._log("[RUN_LAYOUT] mode=validation layout=core|telemetry|reports")

        send_results = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=True, logger=self._log)
        legacy_file_iuid_map = resolve_run_artifact_path(run_dir, "file_iuid_map.csv", for_write=False, logger=self._log)
        for filename in ["validation_results.csv", "validation_by_iuid.csv", "validation_by_file.csv", "reconciliation_report.csv"]:
            cleanup_run_artifact_variants(run_dir, filename)
        events = resolve_run_artifact_path(run_dir, "events.csv", for_write=True, logger=self._log)
        validation_results = resolve_run_artifact_path(run_dir, "validation_results.csv", for_write=True, logger=self._log)
        recon = resolve_run_artifact_path(run_dir, "reconciliation_report.csv", for_write=True, logger=self._log)

        send_rows = read_csv_rows(send_results)
        map_by_file = build_iuid_map_from_send_rows(send_rows)
        merge_iuid_map_from_legacy_file(map_by_file, legacy_file_iuid_map)

        total_send_rows = len(send_rows)
        send_ok_files = sum(1 for r in send_rows if r.get("send_status", "") == "SENT_OK")
        send_warn_files = sum(1 for r in send_rows if r.get("send_status", "") in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"])
        send_fail_files = sum(1 for r in send_rows if r.get("send_status", "") == "SEND_FAIL")
        self._log(f"[VAL_START] run_id={run}")
        self._log(
            f"[VAL_RESULT] send_total={total_send_rows} sent_ok={send_ok_files} "
            f"warn={send_warn_files} fail={send_fail_files}"
        )
        self._log(f"Mapeamentos IUID atuais (send_results+fallback legado): {len(map_by_file)}")
        write_telemetry_event(
            events,
            run,
            "VALIDATION_START",
            "Validacao iniciada.",
            (
                f"send_rows={total_send_rows};sent_ok={send_ok_files};send_warn={send_warn_files};"
                f"send_fail={send_fail_files};mapped_iuid={len(map_by_file)}"
            ),
        )

        updates_by_file: dict[str, dict] = {}
        # consistency check: complete missing IUIDs before API calls
        for row in send_rows:
            if row.get("send_status", "") != "SENT_OK":
                continue
            fp = row.get("file_path", "").strip()
            if not fp or fp in map_by_file:
                continue
            iuid, ts_uid, ts_name, err = self.driver.extract_metadata(self.cfg, Path(fp))
            if iuid:
                map_by_file[fp] = {
                    "sop_instance_uid": iuid,
                    "source_ts_uid": ts_uid,
                    "source_ts_name": ts_name,
                    "extract_status": "CONSISTENCY_OK",
                }
                updates_by_file[fp] = {
                    "sop_instance_uid": iuid,
                    "source_ts_uid": ts_uid,
                    "source_ts_name": ts_name,
                    "extract_status": "CONSISTENCY_OK",
                }
                write_telemetry_event(
                    events,
                    run,
                    "CONSISTENCY_FILLED",
                    "IUID preenchido antes da validacao.",
                    f"file_path={fp}",
                )
            else:
                write_telemetry_event(
                    events,
                    run,
                    "CONSISTENCY_MISSING",
                    err or "Nao foi possivel extrair IUID.",
                    f"file_path={fp}",
                )

        updated_rows = apply_send_result_updates(send_results, run, updates_by_file)
        if updated_rows > 0:
            self._log(f"[CORE_COMPACT] send_results_by_file atualizado pela consistencia em {updated_rows} arquivo(s).")

        iuid_to_files: dict[str, list[str]] = {}
        for row in send_rows:
            if row.get("send_status", "") != "SENT_OK":
                continue
            fp = row.get("file_path", "").strip()
            iuid = str(map_by_file.get(fp, {}).get("sop_instance_uid", "")).strip()
            if not iuid:
                continue
            iuid_to_files.setdefault(iuid, []).append(fp)

        self._log(f"IUIDs unicos para consulta API: {len(iuid_to_files)}")

        validation_fields = [
            "run_id",
            "file_path",
            "sop_instance_uid",
            "send_status",
            "validation_status",
            "api_found",
            "http_status",
            "detail",
            "checked_at",
        ]

        ok_count = 0
        miss_count = 0
        api_err_count = 0
        for iuid, files in iuid_to_files.items():
            if self.cancel_event.is_set():
                raise RuntimeError("Validacao cancelada.")
            url = f"http://{self.cfg.pacs_rest_host}/dcm4chee-arc/aets/{self.cfg.aet_destino}/rs/instances?SOPInstanceUID={iuid}"
            api_found = 0
            http_status = ""
            detail = ""
            try:
                req = urlrequest.Request(url, method="GET")
                with urlrequest.urlopen(req, timeout=20) as resp:
                    http_status = str(resp.status)
                    body = resp.read().decode("utf-8", errors="replace")
                    data = json.loads(body) if body.strip() else []
                    api_found = 1 if isinstance(data, list) and len(data) > 0 else 0
            except urlerror.HTTPError as ex:
                http_status = str(ex.code)
                detail = str(ex)
            except Exception as ex:
                http_status = "ERR"
                detail = str(ex)

            if api_found == 1:
                ok_count += 1
            else:
                if http_status in ["ERR", ""]:
                    api_err_count += 1
                else:
                    miss_count += 1

            status = "OK" if api_found == 1 else ("API_ERROR" if http_status in ["ERR", ""] else "NOT_FOUND")
            for fp in files:
                write_csv_row(
                    validation_results,
                    {
                        "run_id": run,
                        "file_path": fp,
                        "sop_instance_uid": iuid,
                        "send_status": "SENT_OK",
                        "validation_status": status,
                        "api_found": api_found,
                        "http_status": http_status,
                        "detail": detail,
                        "checked_at": now_br(),
                    },
                    validation_fields,
                )
            if (ok_count + miss_count + api_err_count) % 100 == 0:
                self._log(
                    f"Progresso validacao API: {ok_count + miss_count + api_err_count}/{len(iuid_to_files)} "
                    f"(ok={ok_count}, nf={miss_count}, api_err={api_err_count})"
                )

        warnings_count = 0
        fail_count = 0
        for row in send_rows:
            st = row.get("send_status", "")
            if st in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"]:
                warnings_count += 1
            elif st in ["SEND_FAIL"]:
                fail_count += 1

        final_status = "PASS"
        if fail_count > 0 or api_err_count > 0 or miss_count > 0:
            final_status = "PASS_WITH_WARNINGS"
        if api_err_count > 0 and ok_count == 0:
            final_status = "FAIL"

        write_csv_row(
            recon,
            {
                "run_id": run,
                "toolkit": self.cfg.toolkit,
                "total_iuid_unique": len(iuid_to_files),
                "iuid_ok": ok_count,
                "iuid_not_found": miss_count,
                "iuid_api_error": api_err_count,
                "send_warning_files": warnings_count,
                "send_failed_files": fail_count,
                "final_status": final_status,
                "generated_at": now_br(),
            },
            ["run_id", "toolkit", "total_iuid_unique", "iuid_ok", "iuid_not_found", "iuid_api_error", "send_warning_files", "send_failed_files", "final_status", "generated_at"],
        )
        self._log("[VAL_RESULT] --- Resumo Final Validacao ---")
        self._log(f"Run ID: {run}")
        self._log(f"Arquivos do send: {total_send_rows}")
        self._log(f"Arquivos SENT_OK: {send_ok_files}")
        self._log(f"Arquivos com warning no send: {send_warn_files}")
        self._log(f"Arquivos com falha no send: {send_fail_files}")
        self._log(f"IUIDs unicos consultados: {len(iuid_to_files)}")
        self._log(f"IUIDs OK: {ok_count}")
        self._log(f"IUIDs NOT_FOUND: {miss_count}")
        self._log(f"IUIDs API_ERROR: {api_err_count}")
        self._log(f"[VAL_END] run_id={run} status={final_status}")
        write_telemetry_event(
            events,
            run,
            "VALIDATION_END",
            "Validacao finalizada.",
            (
                f"status={final_status};iuid_total={len(iuid_to_files)};iuid_ok={ok_count};"
                f"iuid_not_found={miss_count};iuid_api_error={api_err_count}"
            ),
        )
        return {"run_id": run, "status": final_status, "run_dir": str(run_dir)}


class ConfigDialog(tk.Toplevel):
    def __init__(self, master, config: AppConfig, on_save, on_test_echo):
        super().__init__(master)
        self.title("Configuracoes")
        self.resizable(False, False)
        self.on_save = on_save
        self.on_test_echo = on_test_echo

        self.var_toolkit = tk.StringVar(value=config.toolkit)
        self.var_aet_src = tk.StringVar(value=config.aet_origem)
        self.var_aet_dst = tk.StringVar(value=config.aet_destino)
        self.var_host = tk.StringVar(value=config.pacs_host)
        self.var_port = tk.StringVar(value=str(config.pacs_port))
        self.var_rest = tk.StringVar(value=config.pacs_rest_host)
        self.var_batch = tk.StringVar(value=str(config.batch_size_default))
        self.var_ext = tk.StringVar(value=config.allowed_extensions_csv)
        self.var_include_all_files = tk.BooleanVar(value=not bool(config.restrict_extensions))
        self.var_no_ext = tk.BooleanVar(value=bool(config.include_no_extension))
        self.var_collect_size = tk.BooleanVar(value=bool(config.collect_size_bytes))
        self.var_ts = tk.StringVar(value=config.ts_mode)
        self.var_dcm4che_send_mode = tk.StringVar(value=normalize_dcm4che_send_mode(config.dcm4che_send_mode))

        frm = ttk.Frame(self, padding=12)
        frm.grid(sticky="nsew")
        frm.columnconfigure(1, weight=1)
        self.cmb_toolkit = self._row_entry(frm, 0, "Toolkit", self.var_toolkit, combo_values=["dcm4che", "dcmtk"])
        if isinstance(self.cmb_toolkit, ttk.Combobox):
            self.cmb_toolkit.bind("<<ComboboxSelected>>", lambda _e: self._toggle_dcm4che_controls())
        self._row_entry(frm, 1, "AET origem", self.var_aet_src)
        self._row_entry(frm, 2, "AET destino", self.var_aet_dst)
        self._row_entry(frm, 3, "PACS DICOM host (C-STORE)", self.var_host)
        self._row_entry(frm, 4, "PACS DICOM port (C-STORE)", self.var_port)
        self._row_entry(frm, 5, "PACS REST host:porta", self.var_rest)
        self._row_entry(frm, 6, "Batch default", self.var_batch)
        self.lbl_dcm4che_mode = ttk.Label(frm, text="Modo de envio dcm4che")
        self.lbl_dcm4che_mode.grid(row=7, column=0, sticky="w", pady=3)
        self.cmb_dcm4che_mode = ttk.Combobox(
            frm,
            textvariable=self.var_dcm4che_send_mode,
            values=["MANIFEST_FILES", "FOLDERS"],
            width=56,
            state="readonly",
        )
        self.cmb_dcm4che_mode.grid(row=7, column=1, sticky="we", pady=3)
        if isinstance(self.cmb_dcm4che_mode, ttk.Combobox):
            self.cmb_dcm4che_mode.bind("<<ComboboxSelected>>", lambda _e: self._toggle_extension_controls())

        self.filter_frame = ttk.LabelFrame(frm, text="Filtro de arquivos para analise", padding=8)
        self.filter_frame.grid(row=8, column=0, columnspan=2, sticky="we", pady=(6, 0))
        self.filter_frame.columnconfigure(1, weight=1)
        self.chk_include_all = ttk.Checkbutton(
            self.filter_frame,
            text="Nao restringir por extensao (incluir todos os arquivos)",
            variable=self.var_include_all_files,
            command=self._toggle_extension_controls,
        )
        self.chk_include_all.grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(
            self.filter_frame,
            text="Extensoes permitidas (separadas por virgula, ex: .dcm,.ima)",
        ).grid(row=1, column=0, sticky="w", pady=(6, 3))
        self.entry_ext = ttk.Entry(self.filter_frame, textvariable=self.var_ext, width=58)
        self.entry_ext.grid(row=1, column=1, sticky="we", pady=(6, 3))
        self.chk_no_ext = ttk.Checkbutton(self.filter_frame, text="Incluir arquivos sem extensao", variable=self.var_no_ext)
        self.chk_no_ext.grid(row=2, column=0, columnspan=2, sticky="w")
        self.lbl_filter_mode_hint = ttk.Label(self.filter_frame, text="")
        self.lbl_filter_mode_hint.grid(row=3, column=0, columnspan=2, sticky="w", pady=(4, 0))

        ttk.Checkbutton(
            frm,
            text="Calcular size_bytes na analise (mais lento)",
            variable=self.var_collect_size,
        ).grid(row=9, column=0, columnspan=2, sticky="w")
        self._row_entry(frm, 10, "TS mode", self.var_ts, combo_values=["AUTO", "JPEG_LS_LOSSLESS", "UNCOMPRESSED_STANDARD"])
        self._toggle_dcm4che_controls()

        btns = ttk.Frame(frm)
        btns.grid(row=11, column=0, columnspan=2, pady=(12, 0), sticky="e")
        ttk.Button(btns, text="Testar Echo", command=self._test_echo).pack(side="left", padx=4)
        ttk.Button(btns, text="Salvar", command=self._save).pack(side="left", padx=4)
        ttk.Button(btns, text="Fechar", command=self.destroy).pack(side="left", padx=4)

    def _row_entry(self, parent, row, label, var, browse=False, combo_values=None):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=3)
        if combo_values:
            w = ttk.Combobox(parent, textvariable=var, values=combo_values, width=56, state="readonly")
        else:
            w = ttk.Entry(parent, textvariable=var, width=58)
        w.grid(row=row, column=1, sticky="we", pady=3)
        if browse:
            ttk.Button(parent, text="...", width=3, command=lambda: self._browse(var)).grid(row=row, column=2, padx=(4, 0))
        return w

    def _browse(self, var):
        p = filedialog.askdirectory(parent=self)
        if p:
            var.set(p)

    def _toggle_extension_controls(self):
        if self._is_filter_block_disabled_by_mode():
            self.chk_include_all.configure(state="disabled")
            self.entry_ext.configure(state="disabled")
            self.chk_no_ext.configure(state="disabled")
            self.lbl_filter_mode_hint.configure(text="Modo dcm4che FOLDERS: filtro de arquivos inativo.")
            return
        self.chk_include_all.configure(state="normal")
        state = "disabled" if bool(self.var_include_all_files.get()) else "normal"
        self.entry_ext.configure(state=state)
        self.chk_no_ext.configure(state=state)
        self.lbl_filter_mode_hint.configure(text="")

    def _is_filter_block_disabled_by_mode(self) -> bool:
        return (
            self.var_toolkit.get().strip().lower() == "dcm4che"
            and normalize_dcm4che_send_mode(self.var_dcm4che_send_mode.get().strip()) == "FOLDERS"
        )

    def _toggle_dcm4che_controls(self):
        is_dcm4che = self.var_toolkit.get().strip().lower() == "dcm4che"
        if is_dcm4che:
            self.lbl_dcm4che_mode.grid()
            self.cmb_dcm4che_mode.grid()
        else:
            self.lbl_dcm4che_mode.grid_remove()
            self.cmb_dcm4che_mode.grid_remove()
        self._toggle_extension_controls()

    def _build_config(self) -> AppConfig:
        return AppConfig(
            toolkit=self.var_toolkit.get().strip(),
            aet_origem=self.var_aet_src.get().strip(),
            aet_destino=self.var_aet_dst.get().strip(),
            pacs_host=self.var_host.get().strip(),
            pacs_port=int(self.var_port.get().strip()),
            pacs_rest_host=self.var_rest.get().strip(),
            batch_size_default=int(self.var_batch.get().strip()),
            allowed_extensions_csv=self.var_ext.get().strip(),
            restrict_extensions=not bool(self.var_include_all_files.get()),
            include_no_extension=bool(self.var_no_ext.get()),
            collect_size_bytes=bool(self.var_collect_size.get()),
            ts_mode=self.var_ts.get().strip(),
            dcm4che_send_mode=normalize_dcm4che_send_mode(self.var_dcm4che_send_mode.get().strip()),
        )

    def _test_echo(self):
        try:
            cfg = self._build_config()
            ok, msg = self.on_test_echo(cfg)
            if ok:
                messagebox.showinfo("Echo OK", msg or "Echo executado com sucesso.", parent=self)
            else:
                messagebox.showerror("Echo Falhou", msg or "Falha no echo.", parent=self)
        except Exception as ex:
            messagebox.showerror("Erro", str(ex), parent=self)

    def _save(self):
        try:
            cfg = self._build_config()
        except Exception as ex:
            messagebox.showerror("Erro", f"Configuracao invalida: {ex}", parent=self)
            return
        self.on_save(cfg)
        messagebox.showinfo("OK", "Configuracoes salvas.", parent=self)
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.base_dir = Path(__file__).resolve().parent
        self.app_version = read_app_version(self.base_dir)
        self.title(f"{APP_DISPLAY_NAME} - {self.app_version}")
        self.geometry("1180x760")
        self.config_file = self.base_dir / "app_config.json"
        self.config_obj = self._load_config()
        self.queue: queue.Queue = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()

        self.progress_items_var = tk.StringVar(value="enviando item 0 de 0")
        self.progress_chunks_var = tk.StringVar(value="batch chunk 0 de 0")
        self.analysis_progress_var = tk.StringVar(value="progresso analise: aguardando")
        self.log_filter_options = ["Todos", "Sistema", "Warnings + Erros"]
        self.var_log_filter_an = tk.StringVar(value="Todos")
        self.var_log_filter_send = tk.StringVar(value="Todos")
        self.var_log_filter_val = tk.StringVar(value="Todos")
        self._max_log_buffer_lines = 6000
        self._log_buffers: dict[str, list[tuple[str, str, str]]] = {"an": [], "send": [], "val": []}
        self._log_widgets: dict[str, tk.Text] = {}
        self.activity_status_an = tk.StringVar(value="ocioso")
        self.activity_status_send = tk.StringVar(value="ocioso")
        self.activity_status_val = tk.StringVar(value="ocioso")
        self._activity_context = ""
        self._activity_running = False
        self._activity_bars: list[ttk.Progressbar] = []

        self._build_menu()
        self._setup_ui_styles()
        self._build_ui()
        self._poll_queue()

    def _load_config(self) -> AppConfig:
        dcm4che_bin = find_toolkit_bin(self.base_dir, "dcm4che", "storescu.bat")
        dcmtk_bin = find_toolkit_bin(self.base_dir, "dcmtk", "storescu.exe")
        cfg = AppConfig(
            dcm4che_bin_path=dcm4che_bin,
            dcmtk_bin_path=dcmtk_bin,
        )
        if self.config_file.exists():
            try:
                raw = json.loads(self.config_file.read_text(encoding="utf-8"))
                cfg = AppConfig(**{**asdict(cfg), **raw})
            except Exception:
                pass
        if cfg.aet_origem.strip().upper() == "STORESCU":
            cfg.aet_origem = "HMD_IMPORTER"
        # Keep runs local to the app by default; this setting is no longer exposed in UI.
        cfg.runs_base_dir = ""
        cfg.dcm4che_send_mode = normalize_dcm4che_send_mode(cfg.dcm4che_send_mode)
        apply_internal_toolkit_paths(cfg, self.base_dir)
        return cfg

    def _save_config(self, cfg: AppConfig):
        prev_toolkit = (self.config_obj.toolkit or "").strip().lower()
        prev_mode = normalize_dcm4che_send_mode(self.config_obj.dcm4che_send_mode)
        new_toolkit = (cfg.toolkit or "").strip().lower()
        new_mode = normalize_dcm4che_send_mode(cfg.dcm4che_send_mode)
        mode_changed = (prev_toolkit != new_toolkit) or (prev_toolkit == "dcm4che" and prev_mode != new_mode)

        self.config_obj = cfg
        self.config_file.write_text(json.dumps(asdict(cfg), ensure_ascii=True, indent=2), encoding="utf-8")
        self.var_batch_size.set(str(cfg.batch_size_default))
        self._log_an(
            f"[CFG_SAVE] toolkit={cfg.toolkit} aet_origem={cfg.aet_origem} aet_destino={cfg.aet_destino} "
            f"pacs_dicom={cfg.pacs_host}:{cfg.pacs_port} pacs_rest={cfg.pacs_rest_host} "
            f"batch={cfg.batch_size_default} restrict_extensions={'ON' if cfg.restrict_extensions else 'OFF'} "
            f"include_no_extension={'ON' if cfg.include_no_extension else 'OFF'} "
            f"collect_size_bytes={'ON' if cfg.collect_size_bytes else 'OFF'} "
            f"dcm4che_send_mode={cfg.dcm4che_send_mode}"
        )
        self._log_an("Configuracoes atualizadas.")
        if mode_changed:
            self.var_run_id.set("")
            self._log_an(
                "[RUN_ID_RESET] toolkit/modo alterado; Run ID (opcional) limpo para evitar sufixo incorreto."
            )
            messagebox.showinfo(
                "Configuracao atualizada",
                "Toolkit/modo de envio alterado.\n\n"
                "O campo 'Run ID (opcional)' foi limpo para evitar sufixo incorreto. "
                "Nao e necessario reiniciar o sistema.",
                parent=self,
            )
        self._refresh_run_list()

    def _build_menu(self):
        menu = tk.Menu(self)
        self.config(menu=menu)
        m = tk.Menu(menu, tearoff=0)
        m.add_command(label="Configuracoes", command=self._open_config_dialog)
        m.add_command(label="Atualizar runs", command=self._refresh_run_list)
        menu.add_cascade(label="Configuracao", menu=m)
        menu.add_command(label="Sobre", command=self._show_about)

    def _build_ui(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)
        self.tab_an = ttk.Frame(nb)
        self.tab_send = ttk.Frame(nb)
        self.tab_val = ttk.Frame(nb)
        self.tab_runs = ttk.Frame(nb)
        nb.add(self.tab_an, text="Analise")
        nb.add(self.tab_send, text="Send")
        nb.add(self.tab_val, text="Validacao")
        nb.add(self.tab_runs, text="Runs")
        self._build_analyze_tab()
        self._build_send_tab()
        self._build_validation_tab()
        self._build_runs_tab()

    def _setup_ui_styles(self) -> None:
        style = ttk.Style(self)
        style.configure("Compact.Horizontal.TProgressbar", thickness=6)

    def _build_analyze_tab(self):
        top = ttk.Frame(self.tab_an, padding=10)
        top.pack(fill="x")
        self.var_exam_root = tk.StringVar()
        self.var_batch_size = tk.StringVar(value=str(self.config_obj.batch_size_default))
        self.var_run_id = tk.StringVar()
        ttk.Label(top, text="Pasta exames").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.var_exam_root, width=90).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top, text="...", width=3, command=self._browse_exam_root).grid(row=0, column=2)
        ttk.Label(top, text="Batch size (dcm4che=folders/files conforme modo | dcmtk=arquivos)").grid(row=1, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.var_batch_size, width=12).grid(row=1, column=1, sticky="w", padx=6)
        ttk.Label(top, text="Run ID (opcional)").grid(row=2, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.var_run_id, width=28).grid(row=2, column=1, sticky="w", padx=6)
        btns = ttk.Frame(top)
        btns.grid(row=3, column=0, columnspan=3, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Analisar Pasta", command=self._start_analysis).pack(side="left", padx=4)
        ttk.Button(btns, text="Cancelar", command=self._cancel_current_job).pack(side="left", padx=4)

        dash = ttk.LabelFrame(self.tab_an, text="Dashboard de Analise", padding=10)
        dash.pack(fill="x", padx=10, pady=8)
        self.lbl_dash = tk.StringVar(value="Sem analise executada.")
        ttk.Label(dash, textvariable=self.lbl_dash, justify="left").pack(anchor="w")
        ttk.Label(dash, textvariable=self.analysis_progress_var, justify="left").pack(anchor="w", pady=(6, 0))
        activity = ttk.Frame(self.tab_an, padding=(10, 0, 10, 4))
        activity.pack(fill="x")
        ttk.Label(activity, text="Atividade:").pack(side="left")
        ttk.Label(activity, textvariable=self.activity_status_an).pack(side="left", padx=(6, 10))
        self.pb_activity_an = ttk.Progressbar(
            activity,
            mode="indeterminate",
            length=72,
            style="Compact.Horizontal.TProgressbar",
        )
        self.pb_activity_an.pack(side="left")
        self._activity_bars.append(self.pb_activity_an)
        filter_bar = ttk.Frame(self.tab_an, padding=(10, 0, 10, 4))
        filter_bar.pack(fill="x")
        ttk.Label(filter_bar, text="Filtro de log (tela)").pack(side="left")
        cmb_filter_an = ttk.Combobox(
            filter_bar,
            textvariable=self.var_log_filter_an,
            values=self.log_filter_options,
            width=18,
            state="readonly",
        )
        cmb_filter_an.pack(side="left", padx=(8, 0))
        cmb_filter_an.bind("<<ComboboxSelected>>", lambda _e: self._refresh_log_view("an"))

        log_frame = ttk.Frame(self.tab_an, padding=(10, 0, 10, 10))
        log_frame.pack(fill="both", expand=True)
        self.txt_an = tk.Text(log_frame, wrap="none")
        self.txt_an.pack(side="left", fill="both", expand=True)
        y = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_an.yview)
        y.pack(side="right", fill="y")
        self.txt_an.configure(yscrollcommand=y.set)
        self._setup_log_tags(self.txt_an)
        self._log_widgets["an"] = self.txt_an

    def _build_send_tab(self):
        top = ttk.Frame(self.tab_send, padding=10)
        top.pack(fill="x")
        self.var_send_run = tk.StringVar()
        self.var_show_send_internal = tk.BooleanVar(value=True)
        self.var_show_output = tk.BooleanVar(value=True)
        ttk.Label(top, text="Run ID analisado").grid(row=0, column=0, sticky="w")
        self.cmb_send_runs = ttk.Combobox(top, textvariable=self.var_send_run, width=36)
        self.cmb_send_runs.grid(row=0, column=1, sticky="w", padx=6)
        ttk.Button(top, text="Atualizar", command=self._refresh_run_list).grid(row=0, column=2, padx=4)
        ttk.Checkbutton(
            top,
            text="Exibir mensagens internas do sistema",
            variable=self.var_show_send_internal,
            command=lambda: self._refresh_log_view("send"),
        ).grid(row=1, column=1, sticky="w", padx=6)
        ttk.Checkbutton(
            top,
            text="Exibir output bruto da toolkit (tempo real)",
            variable=self.var_show_output,
            command=lambda: self._refresh_log_view("send"),
        ).grid(row=2, column=1, sticky="w", padx=6)
        btns = ttk.Frame(top)
        btns.grid(row=3, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Iniciar Send", command=self._start_send).pack(side="left", padx=4)
        ttk.Button(btns, text="Cancelar", command=self._cancel_current_job).pack(side="left", padx=4)

        prog = ttk.LabelFrame(self.tab_send, text="Progresso", padding=10)
        prog.pack(fill="x", padx=10, pady=8)
        ttk.Label(prog, textvariable=self.progress_items_var).pack(anchor="w")
        ttk.Label(prog, textvariable=self.progress_chunks_var).pack(anchor="w")
        activity = ttk.Frame(self.tab_send, padding=(10, 0, 10, 4))
        activity.pack(fill="x")
        ttk.Label(activity, text="Atividade:").pack(side="left")
        ttk.Label(activity, textvariable=self.activity_status_send).pack(side="left", padx=(6, 10))
        self.pb_activity_send = ttk.Progressbar(
            activity,
            mode="indeterminate",
            length=72,
            style="Compact.Horizontal.TProgressbar",
        )
        self.pb_activity_send.pack(side="left")
        self._activity_bars.append(self.pb_activity_send)
        filter_bar = ttk.Frame(self.tab_send, padding=(10, 0, 10, 4))
        filter_bar.pack(fill="x")
        ttk.Label(filter_bar, text="Filtro de log (tela)").pack(side="left")
        cmb_filter_send = ttk.Combobox(
            filter_bar,
            textvariable=self.var_log_filter_send,
            values=self.log_filter_options,
            width=18,
            state="readonly",
        )
        cmb_filter_send.pack(side="left", padx=(8, 0))
        cmb_filter_send.bind("<<ComboboxSelected>>", lambda _e: self._refresh_log_view("send"))

        log_frame = ttk.Frame(self.tab_send, padding=(10, 0, 10, 10))
        log_frame.pack(fill="both", expand=True)
        self.txt_send = tk.Text(log_frame, wrap="none")
        self.txt_send.pack(side="left", fill="both", expand=True)
        y = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_send.yview)
        y.pack(side="right", fill="y")
        self.txt_send.configure(yscrollcommand=y.set)
        self._setup_log_tags(self.txt_send)
        self._log_widgets["send"] = self.txt_send

    def _build_validation_tab(self):
        top = ttk.Frame(self.tab_val, padding=10)
        top.pack(fill="x")
        self.var_val_run = tk.StringVar()
        self.var_report_mode = tk.StringVar(value="A - por arquivo")
        ttk.Label(top, text="Run ID").grid(row=0, column=0, sticky="w")
        self.cmb_val_runs = ttk.Combobox(top, textvariable=self.var_val_run, width=40)
        self.cmb_val_runs.grid(row=0, column=1, sticky="w", padx=6)
        ttk.Button(top, text="Atualizar", command=self._refresh_run_list).grid(row=0, column=2, padx=4)
        ttk.Button(top, text="Validar Run", command=self._start_validation).grid(row=0, column=3, padx=4)
        ttk.Button(top, text="Cancelar", command=self._cancel_current_job).grid(row=0, column=4, padx=4)
        ttk.Label(top, text="Modo relatorio").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(
            top,
            textvariable=self.var_report_mode,
            values=["A - por arquivo", "C - por StudyUID"],
            width=24,
            state="readonly",
        ).grid(row=1, column=1, sticky="w", padx=6, pady=(8, 0))
        ttk.Button(top, text="Exportar relatorio completo", command=self._start_export_report).grid(row=1, column=3, padx=4, pady=(8, 0))
        activity = ttk.Frame(self.tab_val, padding=(10, 0, 10, 4))
        activity.pack(fill="x")
        ttk.Label(activity, text="Atividade:").pack(side="left")
        ttk.Label(activity, textvariable=self.activity_status_val).pack(side="left", padx=(6, 10))
        self.pb_activity_val = ttk.Progressbar(
            activity,
            mode="indeterminate",
            length=72,
            style="Compact.Horizontal.TProgressbar",
        )
        self.pb_activity_val.pack(side="left")
        self._activity_bars.append(self.pb_activity_val)
        filter_bar = ttk.Frame(self.tab_val, padding=(10, 0, 10, 4))
        filter_bar.pack(fill="x")
        ttk.Label(filter_bar, text="Filtro de log (tela)").pack(side="left")
        cmb_filter_val = ttk.Combobox(
            filter_bar,
            textvariable=self.var_log_filter_val,
            values=self.log_filter_options,
            width=18,
            state="readonly",
        )
        cmb_filter_val.pack(side="left", padx=(8, 0))
        cmb_filter_val.bind("<<ComboboxSelected>>", lambda _e: self._refresh_log_view("val"))
        log_frame = ttk.Frame(self.tab_val, padding=(10, 0, 10, 10))
        log_frame.pack(fill="both", expand=True)
        self.txt_val = tk.Text(log_frame, wrap="none")
        self.txt_val.pack(side="left", fill="both", expand=True)
        y = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_val.yview)
        y.pack(side="right", fill="y")
        self.txt_val.configure(yscrollcommand=y.set)
        self._setup_log_tags(self.txt_val)
        self._log_widgets["val"] = self.txt_val

    def _build_runs_tab(self):
        root = ttk.Frame(self.tab_runs, padding=10)
        root.pack(fill="both", expand=True)
        top = ttk.Frame(root)
        top.pack(fill="x")
        ttk.Button(top, text="Atualizar runs", command=self._refresh_run_list).pack(side="left", padx=4)
        ttk.Button(top, text="Abrir pasta selecionada", command=self._open_selected_run_folder).pack(side="left", padx=4)
        self.lst_runs = tk.Listbox(root)
        self.lst_runs.pack(fill="both", expand=True, pady=(8, 0))
        self._refresh_run_list()

    def _runs_base(self) -> Path:
        if self.config_obj.runs_base_dir.strip():
            p = Path(self.config_obj.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (self.base_dir / p).resolve()
        return (self.base_dir / "runs").resolve()

    def _refresh_run_list(self):
        runs_base = self._runs_base()
        runs_base.mkdir(parents=True, exist_ok=True)
        runs = [p.name for p in runs_base.iterdir() if p.is_dir()]
        runs.sort(reverse=True)
        self.cmb_send_runs["values"] = runs
        self.cmb_val_runs["values"] = runs
        self.lst_runs.delete(0, tk.END)
        for r in runs:
            self.lst_runs.insert(tk.END, r)

    def _open_selected_run_folder(self):
        sel = self.lst_runs.curselection()
        if not sel:
            return
        run_id = self.lst_runs.get(sel[0])
        p = self._runs_base() / run_id
        if p.exists():
            os.startfile(str(p))

    def _open_config_dialog(self):
        ConfigDialog(self, self.config_obj, self._save_config, self._test_echo)

    def _show_about(self):
        messagebox.showinfo(
            "Sobre",
            f"{APP_DISPLAY_NAME}\nVersao: {self.app_version}\n\nFluxo DICOM com dcm4che e DCMTK.",
        )

    def _test_echo(self, cfg: AppConfig) -> tuple[bool, str]:
        try:
            apply_internal_toolkit_paths(cfg, self.base_dir)
            cmd = get_driver(cfg.toolkit).echo_cmd(cfg)
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False, **hidden_process_kwargs())
            out = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
            return (proc.returncode == 0), (out or f"exit={proc.returncode}")
        except Exception as ex:
            return False, str(ex)

    def _worker_busy(self) -> bool:
        return self.worker_thread is not None and self.worker_thread.is_alive()

    def _set_activity_context(self, context: str) -> None:
        self._activity_context = (context or "").strip()

    def _set_activity_running(self, running: bool) -> None:
        if running:
            status = f"processando ({self._activity_context})..." if self._activity_context else "processando..."
        else:
            status = "ocioso"
        self.activity_status_an.set(status)
        self.activity_status_send.set(status)
        self.activity_status_val.set(status)

        if running and not self._activity_running:
            for bar in self._activity_bars:
                bar.start(12)
        elif not running and self._activity_running:
            for bar in self._activity_bars:
                bar.stop()
            self._activity_context = ""
        self._activity_running = running

    def _sync_activity_indicator(self) -> None:
        self._set_activity_running(self._worker_busy())

    def _browse_exam_root(self):
        p = filedialog.askdirectory(parent=self)
        if p:
            self.var_exam_root.set(p)

    def _start_analysis(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ja existe um processo em execucao.")
            return
        exam_root = self.var_exam_root.get().strip()
        if not exam_root:
            messagebox.showerror("Erro", "Informe a pasta de exames.")
            return
        try:
            batch = int(self.var_batch_size.get().strip())
        except Exception:
            messagebox.showerror("Erro", "Batch size invalido.")
            return
        run_id = self.var_run_id.get().strip()
        self.cancel_event.clear()
        self._log_an("Iniciando analise...")
        self.analysis_progress_var.set("progresso analise: iniciando...")
        self._set_activity_context("Analise")
        self._set_activity_running(True)

        def task():
            try:
                wf = AnalyzeWorkflow(
                    self.config_obj,
                    lambda m: self.queue.put(("an_log", m)),
                    self.cancel_event,
                    lambda p: self.queue.put(("an_progress", p)),
                )
                result = wf.run_analysis(exam_root=exam_root, batch_size=batch, run_id=run_id)
                self.queue.put(("an_done", result))
            except WorkflowCancelled as ex:
                self.queue.put(("an_cancelled", str(ex)))
            except Exception as ex:
                self.queue.put(("an_error", str(ex)))

        self.worker_thread = threading.Thread(target=task, daemon=True)
        self.worker_thread.start()

    def _start_send(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ja existe um processo em execucao.")
            return
        run_id = self.var_send_run.get().strip()
        if not run_id:
            messagebox.showerror("Erro", "Informe o run_id.")
            return
        try:
            batch = int(self.var_batch_size.get().strip())
        except Exception:
            messagebox.showerror("Erro", "Batch size invalido.")
            return
        self.cancel_event.clear()
        self._log_send("Iniciando envio...")
        self.progress_items_var.set("enviando item 0 de 0")
        self.progress_chunks_var.set("batch chunk 0 de 0")
        show_output = bool(self.var_show_output.get())
        self._set_activity_context("Send")
        self._set_activity_running(True)

        def progress(items_done, items_total, chunk_no, chunk_total):
            self.queue.put(("send_progress", (items_done, items_total, chunk_no, chunk_total)))

        def task():
            try:
                wf = SendWorkflow(
                    self.config_obj,
                    lambda m: self.queue.put(("send_log_internal", m)),
                    self.cancel_event,
                    progress,
                    toolkit_logger=lambda m: self.queue.put(("send_log_toolkit", m)),
                )
                result = wf.run_send(run_id=run_id, batch_size=batch, show_output=show_output)
                self.queue.put(("send_done", result))
            except Exception as ex:
                self.queue.put(("send_error", str(ex)))

        self.worker_thread = threading.Thread(target=task, daemon=True)
        self.worker_thread.start()

    def _start_validation(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ja existe um processo em execucao.")
            return
        run_id = self.var_val_run.get().strip()
        if not run_id:
            messagebox.showerror("Erro", "Informe run_id.")
            return
        self.cancel_event.clear()
        self._log_val("[VAL_START] Iniciando validacao...")
        self._set_activity_context("Validacao")
        self._set_activity_running(True)

        def task():
            try:
                wf = ValidationWorkflow(self.config_obj, lambda m: self.queue.put(("val_log", m)), self.cancel_event)
                result = wf.run_validation(run_id=run_id)
                self.queue.put(("val_done", result))
            except Exception as ex:
                self.queue.put(("val_error", str(ex)))

        self.worker_thread = threading.Thread(target=task, daemon=True)
        self.worker_thread.start()

    def _start_export_report(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ja existe um processo em execucao.")
            return
        run_id = self.var_val_run.get().strip()
        if not run_id:
            messagebox.showerror("Erro", "Informe run_id.")
            return
        mode_label = self.var_report_mode.get().strip()
        mode = "A" if mode_label.upper().startswith("A") else "C"
        self.cancel_event.clear()
        self._log_val(f"[REPORT_START] Iniciando exportacao do relatorio completo (modo {mode})...")
        self._set_activity_context("Exportacao de relatorio")
        self._set_activity_running(True)

        def task():
            try:
                wf = ValidationWorkflow(self.config_obj, lambda m: self.queue.put(("val_log", m)), self.cancel_event)
                result = wf.export_complete_report(run_id=run_id, report_mode=mode)
                self.queue.put(("report_done", result))
            except Exception as ex:
                self.queue.put(("report_error", str(ex)))

        self.worker_thread = threading.Thread(target=task, daemon=True)
        self.worker_thread.start()

    def _cancel_current_job(self):
        if not self._worker_busy():
            return
        self.cancel_event.set()
        self._log_an("Cancelamento solicitado...")
        self._log_send("Cancelamento solicitado...")
        self._log_val("Cancelamento solicitado...")

    def _setup_log_tags(self, widget: tk.Text) -> None:
        # Visual-only highlighting in UI. Raw log files remain plain text.
        widget.tag_configure("log_error", foreground="#b00020")
        widget.tag_configure("log_warn", foreground="#8a4b00")
        widget.tag_configure("log_success", foreground="#146c2e")
        widget.tag_configure("log_system", foreground="#0052cc")

    def _classify_log_tag(self, text: str) -> str:
        line = (text or "").strip()
        up = line.upper()
        if not line:
            return ""
        if any(tag in up for tag in ["[AN_END]", "[AN_RESULT]", "[SEND_RESULT]", "[VAL_END]", "[VAL_RESULT]", "[REPORT_EXPORT]"]):
            return "log_success"
        if any(
            tag in up
            for tag in [
                "[AN_START]",
                "[SEND_CONFIG]",
                "[SEND_START]",
                "[CHUNK_START]",
                "[CHUNK_END]",
                "[SEND_END]",
                "[VAL_START]",
                "[CFG_SAVE]",
                "[SEND_PARSE_UID_EMPTY_EXPECTED]",
                "[REPORT_START]",
            ]
        ):
            return "log_system"
        if "[ERRO]" in up or "[ERROR]" in up or "TRACEBACK" in up or "EXCEPTION" in up or "RUNTIMEERROR" in up:
            return "log_error"
        if (
            "[WARN" in up
            or " WARN " in up
            or "PASS_WITH_WARNINGS" in up
            or "SEND_PARSE_" in up
            or "[SEND_WARN_SUMMARY]" in up
            or "SENT_UNKNOWN" in up
        ):
            return "log_warn"
        if (
            "STATUS: PASS" in up
            or "STATUS FINAL: PASS" in up
            or "VALIDACAO FINALIZADA" in up
            or "SEND FINALIZADO" in up
            or "ANALISE FINALIZADA" in up
            or "[REPORT_EXPORT]" in up
        ):
            return "log_success"
        if line.startswith("[") and "]" in line:
            return "log_system"
        return ""

    def _log_filter_mode(self, panel: str) -> str:
        if panel == "an":
            return self.var_log_filter_an.get().strip() or "Todos"
        if panel == "send":
            return self.var_log_filter_send.get().strip() or "Todos"
        if panel == "val":
            return self.var_log_filter_val.get().strip() or "Todos"
        return "Todos"

    def _line_matches_filter(self, panel: str, tag: str, source: str) -> bool:
        if panel == "send":
            if source == "internal" and not bool(self.var_show_send_internal.get()):
                return False
            if source == "toolkit" and not bool(self.var_show_output.get()):
                return False
        mode = self._log_filter_mode(panel)
        if mode == "Todos":
            return True
        if mode == "Sistema":
            return tag == "log_system"
        if mode == "Warnings + Erros":
            return tag in ["log_warn", "log_error"]
        return True

    def _append_widget_line(self, widget: tk.Text, text: str, tag: str) -> None:
        if tag:
            widget.insert("end", text + "\n", tag)
        else:
            widget.insert("end", text + "\n")
        line_count = int(widget.index("end-1c").split(".")[0])
        if line_count > self._max_log_buffer_lines:
            excess = line_count - self._max_log_buffer_lines
            widget.delete("1.0", f"{excess + 1}.0")
        widget.see("end")

    def _refresh_log_view(self, panel: str) -> None:
        widget = self._log_widgets.get(panel)
        if widget is None:
            return
        widget.delete("1.0", "end")
        for text, tag, source in self._log_buffers.get(panel, []):
            if self._line_matches_filter(panel, tag, source):
                self._append_widget_line(widget, text, tag)

    def _append_log_line(self, panel: str, text: str, source: str = "internal") -> None:
        tag = self._classify_log_tag(text)
        buf = self._log_buffers.setdefault(panel, [])
        buf.append((text, tag, source))
        if len(buf) > self._max_log_buffer_lines:
            del buf[: len(buf) - self._max_log_buffer_lines]
        if not self._line_matches_filter(panel, tag, source):
            return
        widget = self._log_widgets.get(panel)
        if widget is None:
            return
        self._append_widget_line(widget, text, tag)

    def _log_an(self, text: str):
        self._append_log_line("an", text)

    def _log_send(self, text: str, source: str = "internal"):
        self._append_log_line("send", text, source=source)

    def _log_val(self, text: str):
        self._append_log_line("val", text)

    def _human_size(self, value: int) -> str:
        n = float(value)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if n < 1024.0:
                return f"{n:.2f} {unit}"
            n /= 1024.0
        return f"{n:.2f} PB"

    def _poll_queue(self):
        try:
            while True:
                event, payload = self.queue.get_nowait()
                if event == "an_log":
                    self._log_an(payload)
                elif event == "an_progress":
                    self.analysis_progress_var.set(payload)
                elif event == "send_log":
                    self._log_send(payload, source="internal")
                elif event == "send_log_internal":
                    self._log_send(payload, source="internal")
                elif event == "send_log_toolkit":
                    self._log_send(payload, source="toolkit")
                elif event == "val_log":
                    self._log_val(payload)
                elif event == "an_done":
                    self._log_an(f"Analise finalizada. Run ID: {payload.get('run_id')}")
                    self.analysis_progress_var.set("progresso analise: finalizada")
                    self.var_send_run.set(payload.get("run_id", ""))
                    self.var_val_run.set(payload.get("run_id", ""))
                    self.var_run_id.set(payload.get("run_id", ""))
                    self._refresh_run_list()
                    self.lbl_dash.set(
                        "Resumo:\n"
                        f"- run_id: {payload.get('run_id')}\n"
                        f"- pastas totais: {payload.get('folders_total')}\n"
                        f"- pastas selecionadas: {payload.get('folders_selected')}\n"
                        f"- arquivos totais: {payload.get('files_total')}\n"
                        f"- arquivos selecionados: {payload.get('files_selected')}\n"
                        f"- tamanho total: {self._human_size(int(payload.get('size_total_bytes') or 0))}\n"
                        f"- tamanho selecionado: {self._human_size(int(payload.get('size_selected_bytes') or 0))}\n"
                        f"- chunks estimados: {payload.get('chunks_total')} ({payload.get('chunk_unit')})"
                    )
                elif event == "send_progress":
                    done, total, cno, ctot = payload
                    self.progress_items_var.set(f"enviando item {done} de {total}")
                    self.progress_chunks_var.set(f"batch chunk {cno} de {ctot}")
                elif event == "send_done":
                    status = payload.get("status")
                    if status == "ALREADY_SENT_PASS":
                        self._log_send(f"RUN ja enviado com sucesso anteriormente. Run ID: {payload.get('run_id')}")
                    else:
                        self._log_send(f"SEND finalizado. Run ID: {payload.get('run_id')} | Status: {status}")
                    self._refresh_run_list()
                elif event == "val_done":
                    self._log_val(f"[VAL_END] Run ID: {payload.get('run_id')} | Status: {payload.get('status')}")
                    self._refresh_run_list()
                elif event == "report_done":
                    self._log_val(
                        "RELATORIO exportado. "
                        f"Run ID: {payload.get('run_id')} | Modo: {payload.get('mode')} | "
                        f"Linhas: {payload.get('rows')} | OK: {payload.get('ok')} | ERRO: {payload.get('erro')}\n"
                        f"Arquivo: {payload.get('report_file')}"
                    )
                    self._refresh_run_list()
                elif event == "an_error":
                    self._log_an(f"[ERRO] {payload}")
                    self.analysis_progress_var.set("progresso analise: erro")
                    messagebox.showerror("Erro na Analise", payload)
                elif event == "an_cancelled":
                    self._log_an(payload)
                    self.analysis_progress_var.set("progresso analise: cancelado")
                elif event == "send_error":
                    self._log_send(f"[ERRO] {payload}")
                    messagebox.showerror("Erro no SEND", payload)
                elif event == "val_error":
                    self._log_val(f"[ERRO] {payload}")
                    messagebox.showerror("Erro na VALIDACAO", payload)
                elif event == "report_error":
                    self._log_val(f"[ERRO] {payload}")
                    messagebox.showerror("Erro na exportacao do relatorio", payload)
        except queue.Empty:
            pass
        self._sync_activity_indicator()
        self.after(120, self._poll_queue)


if __name__ == "__main__":
    app = App()
    app.mainloop()

