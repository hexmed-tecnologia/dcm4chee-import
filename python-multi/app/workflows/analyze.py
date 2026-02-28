import csv
import math
import os
import threading
import time
from pathlib import Path

from app.config.settings import AppConfig
from app.domain.constants import CSV_SEP
from app.infra.run_artifacts import (
    cleanup_run_artifact_variants,
    resolve_run_artifact_path,
    write_csv_row,
    write_telemetry_event,
)
from app.shared.utils import (
    WorkflowCancelled,
    _windows_cmdline_arg_len,
    estimate_dcm4che_batch_max_cmd,
    format_duration_sec,
    format_eta,
    normalize_dcm4che_send_mode,
    now_br,
    now_dual_timestamp,
    now_run_id,
    parse_extensions,
    strip_known_run_suffixes,
    toolkit_run_suffix,
)


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
        analysis_start_ts = time.monotonic()
        # Keep original behavior from monolithic app.py where base dir was project root.
        script_dir = Path(__file__).resolve().parent.parent.parent
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
        selected_file_arg_len_max = 0

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
                                if self.cfg.toolkit == "dcm4che" and dcm4che_send_mode != "FOLDERS":
                                    selected_file_arg_len_max = max(selected_file_arg_len_max, _windows_cmdline_arg_len(entry.path))
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
        analysis_duration_sec = round(max(time.monotonic() - analysis_start_ts, 0.0), 3)
        batch_max_cmd = ""
        batch_max_cmd_source = "N/A"
        if self.cfg.toolkit == "dcm4che":
            if use_folder_unit:
                unit_max_arg_len = 0
                for folder_key in selected_folder_keys:
                    unit_max_arg_len = max(unit_max_arg_len, _windows_cmdline_arg_len(folder_key))
            else:
                unit_max_arg_len = selected_file_arg_len_max
            batch_max_cmd_value, batch_max_cmd_source, cmd_budget = estimate_dcm4che_batch_max_cmd(
                self.cfg,
                unit_max_arg_len=unit_max_arg_len,
                units_total=chunk_base_count,
            )
            batch_max_cmd = str(batch_max_cmd_value)
            self._log(
                f"[BATCH_AUTO_MAX] source={batch_max_cmd_source} limit={batch_max_cmd_value} "
                f"units_total={chunk_base_count} unit_max_arg_len={unit_max_arg_len} budget={cmd_budget}"
            )
        else:
            self._log("[BATCH_AUTO_MAX] source=N/A toolkit=dcmtk")
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
            "analysis_duration_sec",
            "batch_max_cmd",
            "batch_max_cmd_source",
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
                "analysis_duration_sec": analysis_duration_sec,
                "batch_max_cmd": batch_max_cmd,
                "batch_max_cmd_source": batch_max_cmd_source,
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
                f"collect_size_bytes={'1' if self.cfg.collect_size_bytes else '0'};"
                f"batch_max_cmd={batch_max_cmd or 'N/A'};batch_max_cmd_source={batch_max_cmd_source};"
                f"analysis_duration_sec={analysis_duration_sec}"
            ),
        )

        self._log(
            f"[AN_RESULT] arquivos={total_files} selecionados={selected_files} "
            f"pastas_selecionadas={selected_folder_count} chunks={chunk_total} ({chunk_unit}) "
            f"duration={format_duration_sec(analysis_duration_sec)}"
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
            "analysis_duration_sec": analysis_duration_sec,
            "batch_max_cmd": batch_max_cmd,
            "batch_max_cmd_source": batch_max_cmd_source,
        }
