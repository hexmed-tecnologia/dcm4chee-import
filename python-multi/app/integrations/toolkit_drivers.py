import subprocess
from pathlib import Path

from app.config.settings import AppConfig
from app.domain.constants import (
    DCM4CHE_STORE_RQ_RE,
    DCM4CHE_STORE_RSP_ERR_RE,
    DCM4CHE_STORE_RSP_OK_RE,
    DCMTK_BAD_FILE_RE,
    DCMTK_NO_SOP_UID_RE,
    DCMTK_SENDING_FILE_RE,
    DCMTK_STORE_FAILED_FILE_RE,
    DCMTK_STORE_FAILED_REASON_RE,
    DCMTK_STORE_RSP_RE,
    UID_TAG_0002_0010,
    UID_TAG_0008_0018,
)
from app.shared.utils import hidden_process_kwargs, normalize_uid_candidate


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
        iuid = normalize_uid_candidate(iuid_m.group(1) if iuid_m else "")
        ts_uid = normalize_uid_candidate(ts_m.group(1) if ts_m else "")
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
        iuid = normalize_uid_candidate(iuid_m.group(1) if iuid_m else "")
        ts_uid = normalize_uid_candidate(ts_m.group(1) if ts_m else "")
        return iuid, ts_uid, ts_uid, ""

    def parse_send_output(self, lines: list[str], batch_files: list[Path]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        current_file = ""
        pending_failed_file = ""
        for line in lines:
            m_file = DCMTK_SENDING_FILE_RE.search(line)
            if m_file:
                current_file = m_file.group(1).strip()
                result.setdefault(
                    current_file,
                    {"send_status": "SENT_UNKNOWN", "status_detail": "File sending initiated; awaiting response"},
                )
                pending_failed_file = ""
                continue
            m_bad = DCMTK_BAD_FILE_RE.search(line)
            if m_bad:
                bad_file = m_bad.group(1).strip()
                detail = m_bad.group(2).strip()
                result[bad_file] = {"send_status": "NON_DICOM", "status_detail": detail}
                pending_failed_file = ""
                continue
            m_no_sop = DCMTK_NO_SOP_UID_RE.search(line)
            if m_no_sop:
                bad_file = m_no_sop.group(1).strip()
                result[bad_file] = {
                    "send_status": "SENT_UNKNOWN",
                    "status_detail": "No SOP Class or Instance UID in file",
                }
                pending_failed_file = ""
                current_file = bad_file
                continue
            m_failed_file = DCMTK_STORE_FAILED_FILE_RE.search(line)
            if m_failed_file:
                pending_failed_file = m_failed_file.group(1).strip()
                result[pending_failed_file] = {
                    "send_status": "SENT_UNKNOWN",
                    "status_detail": "Store failed; awaiting reason line",
                }
                current_file = pending_failed_file
                continue
            m_failed_reason = DCMTK_STORE_FAILED_REASON_RE.search(line)
            if m_failed_reason and pending_failed_file:
                detail = m_failed_reason.group(1).strip()
                result[pending_failed_file] = {
                    "send_status": "SENT_UNKNOWN",
                    "status_detail": detail,
                }
                pending_failed_file = ""
                continue
            m_rsp = DCMTK_STORE_RSP_RE.search(line)
            if m_rsp and current_file:
                detail = m_rsp.group(1).strip()
                status = "SENT_OK" if "Success" in detail else "SEND_FAIL"
                if ("Unknown Status: 0x110" in detail) and Path(current_file).name.upper() == "DICOMDIR":
                    status = "UNSUPPORTED_DICOM_OBJECT"
                result[current_file] = {"send_status": status, "status_detail": detail}
                pending_failed_file = ""
        for p in batch_files:
            k = str(p)
            result.setdefault(
                k,
                {
                    "send_status": "SENT_UNKNOWN",
                    "status_detail": "parse_status=UNKNOWN;reason=no_match_in_output",
                },
            )
        return result


def get_driver(toolkit: str) -> ToolkitDriver:
    if toolkit == "dcmtk":
        return DcmtkDriver()
    return Dcm4cheDriver()
