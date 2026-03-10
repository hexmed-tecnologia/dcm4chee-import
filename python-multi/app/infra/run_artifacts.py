import csv
import re
from pathlib import Path

from app.domain.constants import CSV_SEP
from app.shared.utils import now_dual_timestamp, now_iso


_INTERNAL_TEXT_ROTATE_MAX_BYTES = 250 * 1024 * 1024


def set_internal_text_rotate_max_mb(max_mb: int) -> int:
    global _INTERNAL_TEXT_ROTATE_MAX_BYTES
    try:
        normalized_mb = max(1, int(max_mb))
    except Exception:
        normalized_mb = 250
    _INTERNAL_TEXT_ROTATE_MAX_BYTES = normalized_mb * 1024 * 1024
    return _INTERNAL_TEXT_ROTATE_MAX_BYTES


def list_incremental_rotated_paths(path: Path) -> list[Path]:
    parent = path.parent
    if not parent.exists():
        return []
    stem = path.stem
    suffix = path.suffix
    rx = re.compile(rf"^{re.escape(stem)}_(\d+){re.escape(suffix)}$")
    indexed: list[tuple[int, Path]] = []
    for candidate in parent.iterdir():
        if not candidate.is_file():
            continue
        m = rx.match(candidate.name)
        if not m:
            continue
        try:
            idx = int(m.group(1))
        except Exception:
            continue
        if idx >= 2:
            indexed.append((idx, candidate))
    indexed.sort(key=lambda x: x[0])
    return [p for _idx, p in indexed]


def next_incremental_rotated_path(path: Path) -> Path:
    highest = 1
    for seg in list_incremental_rotated_paths(path):
        name = seg.name
        base = f"{path.stem}_"
        if not name.startswith(base):
            continue
        raw = name[len(base) : len(name) - len(path.suffix)] if path.suffix else name[len(base) :]
        try:
            highest = max(highest, int(raw))
        except Exception:
            continue
    next_idx = highest + 1
    return path.with_name(f"{path.stem}_{next_idx}{path.suffix}")


def rotate_text_artifact_if_needed(path: Path, max_bytes: int, logger=None) -> Path | None:
    if max_bytes < 1:
        return None
    if not path.exists():
        return None
    try:
        current_size = path.stat().st_size
    except Exception:
        return None
    if current_size < max_bytes:
        return None
    rotated = next_incremental_rotated_path(path)
    try:
        path.rename(rotated)
        if logger:
            logger(f"[ARTIFACT_ROTATE] file={path} rotated_to={rotated} max_bytes={max_bytes}")
        return rotated
    except Exception as ex:
        if logger:
            logger(f"[ARTIFACT_ROTATE_WARN] file={path} error={ex}")
    return None


def _artifact_exists_or_has_segments(path: Path) -> bool:
    return path.exists() or bool(list_incremental_rotated_paths(path))


def _maybe_rotate_internal_text(path: Path) -> None:
    rotate_text_artifact_if_needed(path, _INTERNAL_TEXT_ROTATE_MAX_BYTES)


def append_csv_rows(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    _maybe_rotate_internal_text(path)
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

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=active_fields, delimiter=CSV_SEP)
        if write_header:
            writer.writeheader()
        for row in rows:
            row_data = dict(row)
            if "timestamp_br" in active_fields and "timestamp_br" not in row_data:
                ts_br, ts_iso = now_dual_timestamp()
                row_data["timestamp_br"] = ts_br
                row_data["timestamp_iso"] = ts_iso
            writer.writerow({k: row_data.get(k, "") for k in active_fields})


def write_csv_row(path: Path, row: dict, fieldnames: list[str]) -> None:
    append_csv_rows(path, [row], fieldnames)


def read_csv_rows(path: Path) -> list[dict]:
    ordered_segments = list_incremental_rotated_paths(path)
    ordered_files = [*ordered_segments]
    if path.exists():
        ordered_files.append(path)
    if not ordered_files:
        return []
    rows: list[dict] = []
    for fp in ordered_files:
        try:
            with fp.open("r", newline="", encoding="utf-8", errors="replace") as f:
                rows.extend(list(csv.DictReader(f, delimiter=CSV_SEP)))
        except Exception:
            continue
    return rows


def write_csv_table(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _maybe_rotate_internal_text(path)
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
    "send_results_by_file_trace.csv": RUN_SUBDIR_CORE,
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
        if _artifact_exists_or_has_segments(categorized_path):
            chosen = categorized_path
            source = "categorized_existing"
        elif keep_legacy_on_write and _artifact_exists_or_has_segments(legacy_path):
            chosen = legacy_path
            source = "legacy_existing"
        chosen.parent.mkdir(parents=True, exist_ok=True)
    else:
        if _artifact_exists_or_has_segments(categorized_path):
            chosen = categorized_path
            source = "categorized_existing"
        elif _artifact_exists_or_has_segments(legacy_path):
            chosen = legacy_path
            source = "legacy_existing"
    if logger:
        logger(f"[RUN_PATH_RESOLVE] mode={'write' if for_write else 'read'} file={filename} source={source} path={chosen}")
    return chosen


def cleanup_run_artifact_variants(run_dir: Path, filename: str) -> None:
    categorized_path, legacy_path = run_artifact_variants(run_dir, filename)
    for base in [categorized_path, legacy_path]:
        for p in [*list_incremental_rotated_paths(base), base]:
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
    _maybe_rotate_internal_text(path)
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
