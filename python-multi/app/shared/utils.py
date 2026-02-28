import os
import re
import shlex
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from app.config.settings import AppConfig
from app.domain.constants import IS_WINDOWS, UID_VALUE_RE, WINDOWS_CMD_SAFE_MAX_CHARS, WINDOWS_DIRECT_SAFE_MAX_CHARS


def hidden_process_kwargs() -> dict:
    if not IS_WINDOWS:
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {"startupinfo": startupinfo, "creationflags": subprocess.CREATE_NO_WINDOW}


def _windows_cmdline_arg_len(arg: str) -> int:
    return len(subprocess.list2cmdline([str(arg)]))


def _windows_cmdline_len(args: list[str]) -> int:
    return len(subprocess.list2cmdline([str(x) for x in args]))


def _java_argfile_token(token: str) -> str:
    # Java @argfile treats backslash as escape; keep Windows paths literal by doubling "\".
    escaped = str(token).replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{escaped}\""


def format_command_line(args: list[str]) -> str:
    if IS_WINDOWS:
        return subprocess.list2cmdline([str(x) for x in args])
    return " ".join(shlex.quote(str(x)) for x in args)


def command_line_len(args: list[str]) -> int:
    if IS_WINDOWS:
        return _windows_cmdline_len(args)
    return len(format_command_line(args))


def resolve_java_executable() -> tuple[str, str]:
    candidates: list[str] = []
    java_home = os.environ.get("JAVA_HOME", "").strip()
    if java_home:
        java_bin = Path(java_home) / "bin" / ("java.exe" if IS_WINDOWS else "java")
        candidates.append(str(java_bin))
    java_on_path = shutil.which("java")
    if java_on_path:
        candidates.append(java_on_path)

    seen: set[str] = set()
    last_reason = "java_not_found"
    for candidate in candidates:
        key = str(Path(candidate))
        if key in seen:
            continue
        seen.add(key)
        try:
            proc = subprocess.run(
                [candidate, "-version"],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
                **hidden_process_kwargs(),
            )
            if proc.returncode == 0:
                return candidate, "OK"
            last_reason = f"java_version_exit={proc.returncode}"
        except Exception as ex:
            last_reason = str(ex)
    return "", last_reason


def estimate_dcm4che_batch_max_cmd(cfg: AppConfig, unit_max_arg_len: int, units_total: int) -> tuple[int, str, int]:
    if IS_WINDOWS and cfg.dcm4che_prefer_java_direct:
        return units_total, "DCM4CHE_JAVA_ARGFILE", WINDOWS_DIRECT_SAFE_MAX_CHARS
    source = "DCM4CHE_CMD_LIMIT"
    budget = WINDOWS_CMD_SAFE_MAX_CHARS if (IS_WINDOWS and cfg.dcm4che_use_shell_wrapper) else WINDOWS_DIRECT_SAFE_MAX_CHARS
    if units_total <= 0:
        return 0, source, budget
    if unit_max_arg_len <= 0:
        return units_total, source, budget

    try:
        storescu = Path(cfg.dcm4che_bin_path) / "storescu.bat"
        base = [str(storescu), "-c", f"{cfg.aet_destino}@{cfg.pacs_host}:{cfg.pacs_port}"]
        cmd_args = ["cmd", "/c", *base] if cfg.dcm4che_use_shell_wrapper else base
        base_len = _windows_cmdline_len(cmd_args)
    except Exception:
        # Conservative fallback if command assembly fails for any reason.
        storescu_guess = str(Path(cfg.dcm4che_bin_path or "dcm4che/bin") / "storescu.bat")
        base = [storescu_guess, "-c", f"{cfg.aet_destino}@{cfg.pacs_host}:{cfg.pacs_port}"]
        cmd_args = ["cmd", "/c", *base] if cfg.dcm4che_use_shell_wrapper else base
        base_len = _windows_cmdline_len(cmd_args)

    remaining = budget - base_len
    per_unit_cost = 1 + unit_max_arg_len  # 1 space + quoted arg length
    if remaining < per_unit_cost:
        return 0, source, budget
    max_units = remaining // per_unit_cost
    return min(units_total, max_units), source, budget


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


def normalize_uid_candidate(value: str) -> str:
    # dcmdump outputs can contain wrapped UID text; normalize before extracting.
    compact = re.sub(r"\s+", "", (value or "").strip())
    return sanitize_uid(compact)


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


def normalize_dcm4che_iuid_update_mode(mode: str) -> str:
    m = (mode or "").strip().upper()
    if m in ["CHUNK_END", "CHUNK", "BATCH"]:
        return "CHUNK_END"
    return "REALTIME"


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


def format_duration_sec(seconds: float) -> str:
    return f"{max(seconds, 0.0):.1f}s"


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
