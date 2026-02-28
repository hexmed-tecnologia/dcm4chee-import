import csv
from pathlib import Path

from app.domain.constants import CSV_SEP
from app.shared.utils import now_dual_timestamp, now_iso


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
