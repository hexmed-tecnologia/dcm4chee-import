import json
import re
import subprocess
import threading
import time
from pathlib import Path

from app.config.settings import AppConfig
from app.domain.constants import (
    DCM4CHE_CRITICAL_JAR_MARKERS,
    DCM4CHE_JAVA_MAIN_CLASS,
    DCM4CHE_STORE_RQ_RE,
    DCM4CHE_STORE_RSP_ERR_RE,
    DCM4CHE_STORE_RSP_OK_RE,
    DCMTK_BAD_FILE_RE,
    DCMTK_SENDING_FILE_RE,
    DCMTK_STORE_RSP_RE,
    IS_WINDOWS,
    WINDOWS_CMD_SAFE_MAX_CHARS,
    WINDOWS_DIRECT_SAFE_MAX_CHARS,
)
from app.infra.run_artifacts import (
    RUN_SUBDIR_TELEMETRY,
    cleanup_run_artifact_variants,
    read_csv_rows,
    resolve_run_artifact_path,
    resolve_run_batch_args_dir,
    write_csv_row,
    write_telemetry_event,
)
from app.integrations.toolkit_drivers import apply_internal_toolkit_paths, get_driver
from app.shared.utils import (
    _java_argfile_token,
    command_line_len,
    format_command_line,
    format_duration_sec,
    hidden_process_kwargs,
    looks_like_dicom_payload_file,
    normalize_dcm4che_iuid_update_mode,
    normalize_dcm4che_send_mode,
    normalize_uid_candidate,
    now_br,
    resolve_java_executable,
    sanitize_uid,
    send_checkpoint_filename,
)


class SendWorkflow:
    def __init__(self, cfg: AppConfig, logger, cancel_event: threading.Event, progress_callback, toolkit_logger=None):
        self.cfg = cfg
        self.logger = logger
        self.toolkit_logger = toolkit_logger or logger
        self.cancel_event = cancel_event
        self.progress_callback = progress_callback
        self.current_proc: subprocess.Popen | None = None
        # Keep original behavior from monolithic app.py where toolkit root was project root.
        apply_internal_toolkit_paths(self.cfg, Path(__file__).resolve().parent.parent.parent, self._log)
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

    def _dcm4che_storescu_bat_path(self) -> Path:
        if not self.cfg.dcm4che_bin_path:
            raise RuntimeError(
                "storescu.bat nao encontrado na toolkit interna. "
                "Estrutura esperada: <app>\\toolkits\\dcm4che-*\\bin\\storescu.bat"
            )
        storescu = Path(self.cfg.dcm4che_bin_path) / "storescu.bat"
        if not storescu.exists():
            raise RuntimeError(f"storescu.bat nao encontrado: {storescu}")
        return storescu

    def _dcm4che_cmd_budget(self) -> int:
        return WINDOWS_CMD_SAFE_MAX_CHARS if (IS_WINDOWS and self.cfg.dcm4che_use_shell_wrapper) else WINDOWS_DIRECT_SAFE_MAX_CHARS

    def _build_dcm4che_cmd_bat(self, batch_inputs: list[Path]) -> list[str]:
        storescu = self._dcm4che_storescu_bat_path()
        base = [
            str(storescu),
            "-c",
            f"{self.cfg.aet_destino}@{self.cfg.pacs_host}:{self.cfg.pacs_port}",
        ]
        base.extend([str(p) for p in batch_inputs])
        if self.cfg.dcm4che_use_shell_wrapper:
            return ["cmd", "/c", *base]
        return base

    def _split_dcm4che_inputs_by_cmd_limit(self, batch_inputs: list[Path]) -> tuple[list[list[Path]], int, int]:
        budget = self._dcm4che_cmd_budget()
        split_batches: list[list[Path]] = []
        current: list[Path] = []
        max_cmdline_len = 0
        for unit in batch_inputs:
            trial = current + [unit]
            trial_len = command_line_len(self._build_dcm4che_cmd_bat(trial))
            max_cmdline_len = max(max_cmdline_len, trial_len)
            if current and trial_len > budget:
                split_batches.append(current)
                current = [unit]
                single_len = command_line_len(self._build_dcm4che_cmd_bat(current))
                max_cmdline_len = max(max_cmdline_len, single_len)
            else:
                current = trial
        if current:
            split_batches.append(current)
        return split_batches, budget, max_cmdline_len

    def _build_dcm4che_java_cmd(self, java_exec: str, batch_inputs: list[Path], args_file: Path) -> tuple[list[str], Path]:
        if not java_exec:
            raise RuntimeError("java nao encontrado para modo dcm4che JAVA_DIRECT.")
        storescu = self._dcm4che_storescu_bat_path()
        dcm4che_root = storescu.parent.parent
        classpath = dcm4che_root / "lib" / "*"
        java_args_file = args_file.with_suffix(".javaargs")
        tokens = [
            "-cp",
            str(classpath),
            DCM4CHE_JAVA_MAIN_CLASS,
            "-c",
            f"{self.cfg.aet_destino}@{self.cfg.pacs_host}:{self.cfg.pacs_port}",
            *[str(p) for p in batch_inputs],
        ]
        with java_args_file.open("w", encoding="utf-8") as f:
            for token in tokens:
                f.write(f"{_java_argfile_token(token)}\n")
        return [java_exec, f"@{java_args_file}"], java_args_file

    def _check_dcm4che_java_dependencies(self) -> tuple[bool, list[str], Path]:
        storescu = self._dcm4che_storescu_bat_path()
        dcm4che_root = storescu.parent.parent
        lib_dir = dcm4che_root / "lib"
        if not lib_dir.exists():
            return False, [f"lib_dir_not_found:{lib_dir}"], lib_dir

        jar_names = [x.name.lower() for x in lib_dir.glob("*.jar")]
        missing: list[str] = []
        for marker in DCM4CHE_CRITICAL_JAR_MARKERS:
            marker_l = marker.lower()
            if not any(marker_l in jar for jar in jar_names):
                missing.append(marker)
        return len(missing) == 0, missing, lib_dir

    def _write_chunk_command_trace(
        self,
        *,
        trace_file: Path,
        chunk_index: int,
        total_chunks: int,
        cmd_mode: str,
        cmd: list[str],
        cmdline_len: int,
        budget: int,
        args_file: Path,
        java_args_file: Path | None,
    ) -> None:
        trace_file.parent.mkdir(parents=True, exist_ok=True)
        cmdline = format_command_line(cmd)
        with trace_file.open("w", encoding="utf-8") as f:
            f.write(f"chunk={chunk_index}/{total_chunks}\n")
            f.write(f"mode={cmd_mode}\n")
            f.write(f"cmdline_len={cmdline_len}\n")
            f.write(f"budget={budget}\n")
            f.write(f"batch_args_file={args_file}\n")
            if java_args_file is not None:
                f.write(f"java_args_file={java_args_file}\n")
            f.write("\n[command]\n")
            f.write(cmdline)
            f.write("\n")
            if java_args_file is not None and java_args_file.exists():
                f.write("\n[java_args_file_content]\n")
                f.write(java_args_file.read_text(encoding="utf-8", errors="replace"))

    def run_send(self, run_id: str, batch_size: int, show_output: bool = True) -> dict:
        send_start_ts = time.monotonic()
        # Keep original behavior from monolithic app.py where base dir was project root.
        script_dir = Path(__file__).resolve().parent.parent.parent
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
        dcm4che_send_mode = normalize_dcm4che_send_mode(self.cfg.dcm4che_send_mode)
        dcm4che_iuid_update_mode = normalize_dcm4che_iuid_update_mode(self.cfg.dcm4che_iuid_update_mode)
        dcm4che_exec_mode = "N/A"
        dcm4che_exec_reason = "N/A"
        dcm4che_java_exec = ""
        if self.cfg.toolkit == "dcm4che":
            if not self.cfg.dcm4che_prefer_java_direct:
                self._log(
                    "[WARN] dcm4che_prefer_java_direct=OFF ignorado: JAVA_DIRECT agora e obrigatorio para envio."
                )
            dcm4che_java_exec, java_reason = resolve_java_executable()
            if not dcm4che_java_exec:
                self._log(f"[SEND_EXEC_MODE] toolkit=dcm4che mode=JAVA_DIRECT reason=java_unavailable:{java_reason}")
                raise RuntimeError(
                    "JAVA_DIRECT obrigatorio para dcm4che, mas o Java nao esta funcional "
                    f"(motivo: {java_reason}). Instale/ajuste Java 17 e tente novamente."
                )
            dcm4che_exec_mode = "JAVA_DIRECT"
            dcm4che_exec_reason = f"java={dcm4che_java_exec}"
        self._log(
            f"[SEND_CONFIG] toolkit={self.cfg.toolkit} "
            f"dcm4che_send_mode={dcm4che_send_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'} "
            f"dcm4che_iuid_update_mode={dcm4che_iuid_update_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'} "
            f"dcm4che_exec_mode={dcm4che_exec_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'} "
            f"dcm4che_exec_reason={dcm4che_exec_reason if self.cfg.toolkit == 'dcm4che' else 'N/A'}"
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
        send_unit_is_file_mode = not (self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS")
        folder_to_files: dict[str, list[Path]] = {}
        for r in selected_rows:
            folder = str(r.get("folder_path", "")).strip() or str(Path(r["file_path"]).parent)
            folder_to_files.setdefault(folder, []).append(Path(r["file_path"]))

        checkpoint_name = send_checkpoint_filename(self.cfg)
        checkpoint_read = resolve_run_artifact_path(run_dir, checkpoint_name, for_write=False, logger=self._log)
        send_results_read = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=False, logger=self._log)
        send_summary_read = resolve_run_artifact_path(run_dir, "send_summary.csv", for_write=False, logger=self._log)

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

        done_units = max(done_units, 0)
        done_files = max(done_files, 0)

        processed_files_from_results: set[str] = set()
        existing_send_chunk_max = 0
        if send_results_read.exists():
            try:
                for rr in read_csv_rows(send_results_read):
                    fp = str(rr.get("file_path", "")).strip()
                    if fp:
                        processed_files_from_results.add(fp)
                    chunk_no_raw = str(rr.get("chunk_no", "")).strip()
                    if chunk_no_raw:
                        try:
                            existing_send_chunk_max = max(existing_send_chunk_max, int(chunk_no_raw))
                        except Exception:
                            pass
            except Exception:
                processed_files_from_results = set()
                existing_send_chunk_max = 0
        selected_file_set = {str(x) for x in selected}
        done_files_from_results = sum(1 for fp in selected_file_set if fp in processed_files_from_results)
        if send_unit_is_file_mode and done_files_from_results > done_files:
            self._log(
                f"[SEND_RESUME_FROM_RESULTS] done_files_checkpoint={done_files} done_files_results={done_files_from_results}"
            )
            done_files = done_files_from_results
        elif (not send_unit_is_file_mode) and done_files_from_results > 0:
            self._log(
                "[WARN] Resume por send_results_by_file ignora modo FOLDERS; cursor segue checkpoint por unidade."
            )
        done_files = min(done_files, total_items)
        is_resuming = (done_units > 0) or (send_unit_is_file_mode and done_files > 0)

        if not is_resuming:
            for filename in [
                "storescu_execucao.log",
                "send_results_by_file.csv",
                "send_summary.csv",
            ]:
                cleanup_run_artifact_variants(run_dir, filename)
            for legacy_name in ["analysis_events.csv", "send_events.csv", "send_errors.csv", "consistency_events.csv"]:
                cleanup_run_artifact_variants(run_dir, legacy_name)
            self._log(f"RUN_ID envio: {run}")
        else:
            self._log(
                f"[SEND_RESUME_STATE] done_units={done_units} done_files={done_files} "
                f"send_unit_mode={'FILES' if send_unit_is_file_mode else 'FOLDERS'} "
                f"prev_chunk_max={existing_send_chunk_max}"
            )

        log_file = resolve_run_artifact_path(run_dir, "storescu_execucao.log", for_write=True, logger=self._log)
        events = resolve_run_artifact_path(run_dir, "events.csv", for_write=True, logger=self._log)
        send_results = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=True, logger=self._log)
        send_summary = resolve_run_artifact_path(run_dir, "send_summary.csv", for_write=True, logger=self._log)
        checkpoint = resolve_run_artifact_path(run_dir, checkpoint_name, for_write=True, logger=self._log)
        args_dir = resolve_run_batch_args_dir(run_dir, for_write=True, logger=self._log)
        chunk_cmd_dir = run_dir / RUN_SUBDIR_TELEMETRY / "chunk_commands"
        if not is_resuming and chunk_cmd_dir.exists():
            for fp in chunk_cmd_dir.glob("*"):
                if fp.is_file():
                    fp.unlink()
        chunk_cmd_dir.mkdir(parents=True, exist_ok=True)

        if self.cfg.toolkit == "dcm4che":
            self._log(
                f"[SEND_EXEC_MODE] toolkit=dcm4che mode={dcm4che_exec_mode} reason={dcm4che_exec_reason}"
            )
            write_telemetry_event(
                events,
                run,
                "RUN_SEND_MODE",
                "Modo de execucao do envio definido.",
                f"toolkit=dcm4che;mode={dcm4che_exec_mode};reason={dcm4che_exec_reason}",
            )
            jars_ok, missing_jars, jar_lib_dir = self._check_dcm4che_java_dependencies()
            if jars_ok:
                self._log(
                    f"[JAVA_HEALTHCHECK] status=OK lib={jar_lib_dir} "
                    f"critical_markers={','.join(DCM4CHE_CRITICAL_JAR_MARKERS)}"
                )
                write_telemetry_event(
                    events,
                    run,
                    "RUN_SEND_JAVA_HEALTHCHECK",
                    "Dependencias Java criticas validadas.",
                    f"status=OK;lib={jar_lib_dir}",
                )
            else:
                miss = ",".join(missing_jars)
                self._log(
                    f"[JAVA_HEALTHCHECK] status=FAIL lib={jar_lib_dir} missing={miss}"
                )
                write_telemetry_event(
                    events,
                    run,
                    "RUN_SEND_JAVA_HEALTHCHECK",
                    "Dependencias Java criticas ausentes.",
                    f"status=FAIL;lib={jar_lib_dir};missing={miss}",
                )
                raise RuntimeError(
                    "Falha no health-check Java da toolkit dcm4che. "
                    f"JARs criticos ausentes: {miss}. Verifique a pasta {jar_lib_dir}."
                )

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
            raw_chunks = [ordered_folders[i : i + batch_size] for i in range(done_units, units_total, batch_size)]
        else:
            units_total = total_items
            pending_selected = [x for x in selected if str(x) not in processed_files_from_results]
            raw_chunks = [pending_selected[i : i + batch_size] for i in range(0, len(pending_selected), batch_size)]
        pending_items = len(raw_chunks) * batch_size if (self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS") else sum(
            len(x) for x in raw_chunks
        )

        is_already_completed = (units_total > 0 and done_units >= units_total) if not send_unit_is_file_mode else (len(raw_chunks) == 0)
        if is_already_completed:
            prev_status = ""
            if send_summary_read.exists():
                prev_rows = read_csv_rows(send_summary_read)
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

        chunk_start_index = (existing_send_chunk_max + 1) if is_resuming else 1
        if is_resuming:
            write_telemetry_event(
                events,
                run,
                "RUN_SEND_RESUME",
                "Retomada de envio detectada.",
                (
                    f"done_files={done_files};done_units={done_units};pending_items={pending_items};"
                    f"pending_chunks={len(raw_chunks)};chunk_start_index={chunk_start_index};"
                    f"send_unit_mode={'FILES' if send_unit_is_file_mode else 'FOLDERS'}"
                ),
            )
            self._log(
                f"[SEND_RESUME] done_files={done_files} done_units={done_units} "
                f"pending_items={pending_items} pending_chunks={len(raw_chunks)} "
                f"chunk_start_index={chunk_start_index}"
            )
        prepared_chunks: list[tuple[list[Path], list[Path], int, int, int]] = []
        for original_chunk_no, batch in enumerate(raw_chunks, start=chunk_start_index):
            if self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS":
                base_inputs = [Path(x) for x in batch]
                base_files: list[Path] = []
                for folder in batch:
                    base_files.extend(folder_to_files.get(str(folder), []))
            else:
                base_inputs = list(batch)
                base_files = list(batch)

            split_inputs_batches: list[list[Path]] = [base_inputs]
            if self.cfg.toolkit == "dcm4che" and dcm4che_exec_mode == "CMD_BAT":
                split_inputs_batches, split_budget, split_max_len = self._split_dcm4che_inputs_by_cmd_limit(base_inputs)
                if split_max_len > split_budget:
                    self._log(
                        f"[CMDLEN_GUARD_WARN] chunk_origem={original_chunk_no} "
                        f"cmdline_len_max={split_max_len} budget={split_budget} "
                        "ha unidade individual acima do limite; tentativa de envio seguira em unidade minima."
                    )
                if len(split_inputs_batches) > 1:
                    self._log(
                        f"[CHUNK_SPLIT] chunk_origem={original_chunk_no} "
                        f"subchunks={len(split_inputs_batches)} budget={split_budget}"
                    )
                    write_telemetry_event(
                        events,
                        run,
                        "CHUNK_SPLIT_PLAN",
                        "Chunk dividido por limite de linha de comando.",
                        (
                            f"chunk_original={original_chunk_no};subchunks={len(split_inputs_batches)};"
                            f"budget={split_budget};cmdline_len_max={split_max_len}"
                        ),
                    )

            split_total = len(split_inputs_batches)
            for split_pos, split_inputs in enumerate(split_inputs_batches, start=1):
                if self.cfg.toolkit == "dcm4che" and dcm4che_send_mode == "FOLDERS":
                    split_files: list[Path] = []
                    for folder in split_inputs:
                        split_files.extend(folder_to_files.get(str(folder), []))
                else:
                    split_files = list(split_inputs)
                prepared_chunks.append((split_inputs, split_files, original_chunk_no, split_pos, split_total))

        total_chunks = (chunk_start_index - 1) + len(prepared_chunks)

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
                f"dcm4che_send_mode={dcm4che_send_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'};"
                f"dcm4che_iuid_update_mode={dcm4che_iuid_update_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'};"
                f"dcm4che_exec_mode={dcm4che_exec_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'}"
            ),
        )
        self._log(
            f"[SEND_START] total_items={total_items} batch={batch_size} "
            f"toolkit={self.cfg.toolkit} mode={dcm4che_send_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'} "
            f"iuid_mode={dcm4che_iuid_update_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'} "
            f"exec_mode={dcm4che_exec_mode if self.cfg.toolkit == 'dcm4che' else 'N/A'}"
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

        def _write_send_checkpoint(reason: str, file_path: str = "") -> None:
            checkpoint_done_units = item_cursor if send_unit_is_file_mode else unit_cursor
            checkpoint.write_text(
                json.dumps(
                    {
                        "run_id": run,
                        "done_units": checkpoint_done_units,
                        "done_files": item_cursor,
                        "updated_at": now_br(),
                        "checkpoint_mode": "ITEM",
                        "checkpoint_reason": reason,
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            if reason == "ITEM":
                self._log(
                    f"[SEND_CHECKPOINT_ITEM] processed_items={item_cursor}/{total_items} "
                    f"done_units={checkpoint_done_units} file={file_path or 'N/A'}"
                )

        attempt_chunks_total = len(prepared_chunks)
        for chunk_index, (batch_inputs, batch_files, original_chunk_no, split_pos, split_total) in enumerate(
            prepared_chunks, start=chunk_start_index
        ):
            if self.cancel_event.is_set():
                interrupted = True
                break
            attempt_chunk_no = (chunk_index - chunk_start_index) + 1
            batch_file_set = {str(x) for x in batch_files}
            first_item = item_cursor + 1
            last_item = min(item_cursor + len(batch_files), total_items)
            self.progress_callback(
                first_item,
                total_items,
                attempt_chunk_no,
                attempt_chunks_total,
                chunk_index,
                total_chunks,
            )
            split_info = ""
            if split_total > 1:
                split_info = f" split={split_pos}/{split_total} origin={original_chunk_no}"
            self._log(
                f"[CHUNK_START] chunk={chunk_index}/{total_chunks} "
                f"itens={first_item}-{last_item}/{total_items} "
                f"units={len(batch_inputs)} files={len(batch_files)}{split_info}"
            )
            write_telemetry_event(
                events,
                run,
                "CHUNK_START",
                "Chunk iniciado.",
                (
                    f"chunk_no={chunk_index};items={len(batch_files)};units={len(batch_inputs)};"
                    f"exec_mode={dcm4che_exec_mode if self.cfg.toolkit == 'dcm4che' else 'TOOLKIT_DEFAULT'};"
                    f"split_pos={split_pos};split_total={split_total};origin_chunk={original_chunk_no}"
                ),
            )

            args_file = args_dir / f"batch_{chunk_index:06d}.txt"
            with args_file.open("w", encoding="utf-8") as f:
                for file_path in batch_files:
                    f.write(f"\"{file_path}\"\n")

            java_args_file: Path | None = None
            cmd_mode = "TOOLKIT_DEFAULT"
            cmd_budget = WINDOWS_DIRECT_SAFE_MAX_CHARS
            if self.cfg.toolkit == "dcm4che":
                if dcm4che_exec_mode == "JAVA_DIRECT":
                    cmd_mode = "JAVA_DIRECT"
                    cmd, java_args_file = self._build_dcm4che_java_cmd(dcm4che_java_exec, batch_inputs, args_file)
                    self._log(
                        f"[JAVA_ARGFILE_WRITE] chunk={chunk_index}/{total_chunks} file={java_args_file} "
                        "escape=BACKSLASH_ESCAPED_QUOTED"
                    )
                    write_telemetry_event(
                        events,
                        run,
                        "CHUNK_JAVA_ARGFILE",
                        "Arquivo @argfile Java gerado para o chunk.",
                        (
                            f"chunk_no={chunk_index};java_args_file={java_args_file};"
                            "escape=BACKSLASH_ESCAPED_QUOTED"
                        ),
                    )
                else:
                    cmd_mode = "CMD_BAT"
                    cmd = self._build_dcm4che_cmd_bat(batch_inputs)
                    cmd_budget = self._dcm4che_cmd_budget()
            else:
                cmd = self.driver.storescu_cmd(self.cfg, batch_inputs, args_file)

            cmdline_len = command_line_len(cmd)
            command_trace_file = chunk_cmd_dir / f"chunk_{chunk_index:06d}.cmd.txt"
            self._write_chunk_command_trace(
                trace_file=command_trace_file,
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                cmd_mode=cmd_mode,
                cmd=cmd,
                cmdline_len=cmdline_len,
                budget=cmd_budget,
                args_file=args_file,
                java_args_file=java_args_file,
            )
            self._log(
                f"[CHUNK_CMD] chunk={chunk_index}/{total_chunks} mode={cmd_mode} "
                f"cmdline_len={cmdline_len} budget={cmd_budget} trace={command_trace_file}"
            )
            write_telemetry_event(
                events,
                run,
                "CHUNK_CMD_META",
                "Metadados de comando do chunk.",
                (
                    f"chunk_no={chunk_index};mode={cmd_mode};cmdline_len={cmdline_len};budget={cmd_budget};"
                    f"trace={command_trace_file};args_file={args_file};split_pos={split_pos};split_total={split_total};"
                    f"origin_chunk={original_chunk_no}"
                ),
            )
            if cmd_mode == "CMD_BAT" and cmdline_len > cmd_budget:
                write_telemetry_event(
                    events,
                    run,
                    "CHUNK_CMD_OVER_LIMIT",
                    "Comando acima do limite seguro.",
                    f"chunk_no={chunk_index};cmdline_len={cmdline_len};budget={cmd_budget}",
                )
                raise RuntimeError(
                    f"Chunk {chunk_index} excedeu limite seguro de linha de comando: "
                    f"cmdline_len={cmdline_len} budget={cmd_budget}"
                )

            lines: list[str] = []
            exit_code = -1
            realtime_iuid_enabled = (
                self.cfg.toolkit == "dcm4che" and dcm4che_iuid_update_mode == "REALTIME"
            )
            dcmtk_realtime_enabled = self.cfg.toolkit == "dcmtk"
            realtime_written_files: set[str] = set()
            dcmtk_written_files: set[str] = set()
            dcmtk_current_file = ""
            realtime_payload_files = [str(x) for x in batch_files if looks_like_dicom_payload_file(x)]
            realtime_payload_cursor = 0
            realtime_file_by_iuid: dict[str, str] = {}
            realtime_seen_rq_iuids: set[str] = set()
            realtime_seen_rsp_ok_iuids: set[str] = set()
            realtime_seen_rsp_err_iuids: set[str] = set()
            realtime_stream_buffer = ""
            realtime_stream_buffer_max_chars = 200000
            warning_statuses = {"NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"}

            def _write_realtime_iuid_row(
                *,
                file_path_s: str,
                iuid_value: str,
                status_value: str,
                extract_status_value: str,
                detail_suffix: str,
            ) -> None:
                nonlocal item_cursor, sent_ok, warned, failed
                if file_path_s in realtime_written_files:
                    return
                src_iuid = ""
                src_ts_uid = ""
                src_ts_name = ""
                meta_err = ""
                try:
                    src_iuid, src_ts_uid, src_ts_name, meta_err = self.driver.extract_metadata(self.cfg, Path(file_path_s))
                except Exception as ex:
                    meta_err = str(ex)
                src_iuid = sanitize_uid(src_iuid)
                src_ts_uid = sanitize_uid(src_ts_uid)
                src_ts_name = sanitize_uid(src_ts_name)
                observed_iuid = sanitize_uid(iuid_value)
                if observed_iuid:
                    row_iuid = observed_iuid
                elif src_iuid:
                    row_iuid = src_iuid
                else:
                    row_iuid = sanitize_uid(Path(file_path_s).name)

                detail = f"dcm4che realtime_iuid=ON;{detail_suffix}"
                if meta_err:
                    detail += f";meta_err={meta_err}"

                write_csv_row(
                    send_results,
                    {
                        "run_id": run,
                        "file_path": file_path_s,
                        "chunk_no": chunk_index,
                        "toolkit": self.cfg.toolkit,
                        "ts_mode": ts_mode,
                        "send_status": status_value,
                        "status_detail": detail,
                        "sop_instance_uid": row_iuid,
                        "source_ts_uid": src_ts_uid,
                        "source_ts_name": src_ts_name,
                        "extract_status": extract_status_value,
                        "processed_at": now_br(),
                    },
                    result_fields,
                )

                if status_value == "SENT_OK":
                    sent_ok += 1
                elif status_value in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"]:
                    warned += 1
                    warn_type_counts[status_value] = warn_type_counts.get(status_value, 0) + 1
                else:
                    failed += 1

                if status_value != "SENT_OK":
                    write_telemetry_event(
                        events,
                        run,
                        "SEND_FILE_ERROR",
                        detail or status_value,
                        f"chunk_no={chunk_index};file_path={file_path_s};error_type={status_value}",
                    )

                write_telemetry_event(
                    events,
                    run,
                    "SEND_IUID_REALTIME",
                    "IUID registrado em tempo real.",
                    f"chunk_no={chunk_index};file_path={file_path_s};iuid={row_iuid};status={status_value}",
                )
                self._log(
                    f"[SEND_IUID_REALTIME] chunk={chunk_index}/{total_chunks} status={status_value} "
                    f"iuid={row_iuid} file={file_path_s}"
                )
                realtime_written_files.add(file_path_s)
                item_cursor += 1
                self.progress_callback(
                    item_cursor,
                    total_items,
                    attempt_chunk_no,
                    attempt_chunks_total,
                    chunk_index,
                    total_chunks,
                )
                _write_send_checkpoint("ITEM", file_path_s)

            def _process_realtime_stream_line(clean: str) -> None:
                nonlocal realtime_payload_cursor, realtime_stream_buffer
                if not (
                    ("C-STORE-" in clean)
                    or ("iuid=" in clean)
                    or ("status=" in clean)
                ):
                    return
                realtime_stream_buffer += clean + "\n"
                if len(realtime_stream_buffer) > realtime_stream_buffer_max_chars:
                    realtime_stream_buffer = realtime_stream_buffer[-realtime_stream_buffer_max_chars:]

                for m_rq in DCM4CHE_STORE_RQ_RE.finditer(realtime_stream_buffer):
                    rq_iuid = sanitize_uid(m_rq.group(1))
                    if not rq_iuid or rq_iuid in realtime_seen_rq_iuids:
                        continue
                    realtime_seen_rq_iuids.add(rq_iuid)
                    if rq_iuid not in realtime_file_by_iuid:
                        if realtime_payload_cursor < len(realtime_payload_files):
                            mapped_file = realtime_payload_files[realtime_payload_cursor]
                            realtime_payload_cursor += 1
                            realtime_file_by_iuid[rq_iuid] = mapped_file
                            self._log(
                                f"[SEND_IUID_RT_MATCH] chunk={chunk_index}/{total_chunks} kind=RQ "
                                f"iuid={rq_iuid} file={mapped_file}"
                            )
                        else:
                            self._log(
                                f"[SEND_IUID_RT_MISS] chunk={chunk_index}/{total_chunks} kind=RQ "
                                f"iuid={rq_iuid} reason=payload_cursor_exhausted"
                            )

                for m_ok in DCM4CHE_STORE_RSP_OK_RE.finditer(realtime_stream_buffer):
                    rsp_ok_iuid = sanitize_uid(m_ok.group(1))
                    if not rsp_ok_iuid or rsp_ok_iuid in realtime_seen_rsp_ok_iuids:
                        continue
                    realtime_seen_rsp_ok_iuids.add(rsp_ok_iuid)
                    mapped_file = realtime_file_by_iuid.get(rsp_ok_iuid, "")
                    if mapped_file:
                        self._log(
                            f"[SEND_IUID_RT_MATCH] chunk={chunk_index}/{total_chunks} kind=RSP_OK "
                            f"iuid={rsp_ok_iuid} file={mapped_file}"
                        )
                        _write_realtime_iuid_row(
                            file_path_s=mapped_file,
                            iuid_value=rsp_ok_iuid,
                            status_value="SENT_OK",
                            extract_status_value="OK_FROM_STORESCU_REALTIME",
                            detail_suffix="rsp_status=0H",
                        )
                    else:
                        self._log(
                            f"[SEND_IUID_RT_MISS] chunk={chunk_index}/{total_chunks} kind=RSP_OK "
                            f"iuid={rsp_ok_iuid} reason=file_mapping_not_found"
                        )

                for m_err in DCM4CHE_STORE_RSP_ERR_RE.finditer(realtime_stream_buffer):
                    rsp_err_status = (m_err.group(1) or "").strip()
                    rsp_err_iuid = sanitize_uid(m_err.group(2))
                    if not rsp_err_iuid or rsp_err_iuid in realtime_seen_rsp_err_iuids:
                        continue
                    realtime_seen_rsp_err_iuids.add(rsp_err_iuid)
                    mapped_file = realtime_file_by_iuid.get(rsp_err_iuid, "")
                    if mapped_file:
                        self._log(
                            f"[SEND_IUID_RT_MATCH] chunk={chunk_index}/{total_chunks} kind=RSP_ERR "
                            f"iuid={rsp_err_iuid} status={rsp_err_status or 'UNKNOWN'} file={mapped_file}"
                        )
                        _write_realtime_iuid_row(
                            file_path_s=mapped_file,
                            iuid_value=rsp_err_iuid,
                            status_value="SEND_FAIL",
                            extract_status_value="ERR_FROM_STORESCU_REALTIME",
                            detail_suffix=f"rsp_status={rsp_err_status or 'UNKNOWN'}",
                        )
                    else:
                        self._log(
                            f"[SEND_IUID_RT_MISS] chunk={chunk_index}/{total_chunks} kind=RSP_ERR "
                            f"iuid={rsp_err_iuid} status={rsp_err_status or 'UNKNOWN'} reason=file_mapping_not_found"
                        )

            def _write_dcmtk_realtime_row(*, file_path_s: str, status_value: str, detail_value: str) -> None:
                nonlocal item_cursor, sent_ok, warned, failed
                if file_path_s in dcmtk_written_files:
                    return
                if file_path_s not in batch_file_set:
                    self._log(
                        f"[DCMTK_RT_ITEM_MISS] chunk={chunk_index}/{total_chunks} file={file_path_s} "
                        "reason=not_in_batch"
                    )
                    return
                iuid = ""
                ts_uid = ""
                ts_name = ""
                extract_status = ""
                m_err = ""
                try:
                    iuid, ts_uid, ts_name, m_err = self.driver.extract_metadata(self.cfg, Path(file_path_s))
                except Exception as ex:
                    m_err = str(ex)

                if iuid:
                    extract_status = "OK"
                elif status_value == "SENT_OK":
                    extract_status = "MISSING_IUID"
                if m_err and status_value == "SENT_OK":
                    detail_value = (detail_value + " | " + m_err).strip(" |")
                if status_value == "SENT_UNKNOWN" and not detail_value:
                    detail_value = "parse_status=UNKNOWN;reason=no_match_in_output"
                if status_value == "SENT_UNKNOWN":
                    self._log(f"[DCMTK_STATUS_DETAIL_ENRICHED] file={file_path_s} reason={detail_value}")

                write_csv_row(
                    send_results,
                    {
                        "run_id": run,
                        "file_path": file_path_s,
                        "chunk_no": chunk_index,
                        "toolkit": self.cfg.toolkit,
                        "ts_mode": ts_mode,
                        "send_status": status_value,
                        "status_detail": detail_value,
                        "sop_instance_uid": iuid,
                        "source_ts_uid": ts_uid,
                        "source_ts_name": ts_name,
                        "extract_status": extract_status,
                        "processed_at": now_br(),
                    },
                    result_fields,
                )

                if status_value == "SENT_OK":
                    sent_ok += 1
                elif status_value in warning_statuses:
                    warned += 1
                    warn_type_counts[status_value] = warn_type_counts.get(status_value, 0) + 1
                else:
                    failed += 1

                if status_value != "SENT_OK":
                    write_telemetry_event(
                        events,
                        run,
                        "SEND_FILE_ERROR",
                        detail_value or status_value,
                        f"chunk_no={chunk_index};file_path={file_path_s};error_type={status_value}",
                    )

                dcmtk_written_files.add(file_path_s)
                item_cursor += 1
                self._log(
                    f"[DCMTK_RT_ITEM_WRITE] chunk={chunk_index}/{total_chunks} "
                    f"status={status_value} file={file_path_s}"
                )
                self.progress_callback(
                    item_cursor,
                    total_items,
                    attempt_chunk_no,
                    attempt_chunks_total,
                    chunk_index,
                    total_chunks,
                )
                _write_send_checkpoint("ITEM", file_path_s)
                self._log(
                    f"[DCMTK_RT_CHECKPOINT] chunk={chunk_index}/{total_chunks} "
                    f"processed_items={item_cursor}/{total_items} file={file_path_s}"
                )

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
                cancel_watcher_stop = threading.Event()
                cancel_kill_logged = False

                def _cancel_watcher() -> None:
                    nonlocal interrupted, cancel_kill_logged
                    while not cancel_watcher_stop.is_set():
                        proc_ref = self.current_proc
                        if proc_ref is None or proc_ref.poll() is not None:
                            return
                        if self.cancel_event.is_set():
                            if not cancel_kill_logged:
                                cancel_kill_logged = True
                                self._log(
                                    f"[SEND_CANCEL_FORCE_KILL] chunk={chunk_index}/{total_chunks} pid={proc_ref.pid}"
                                )
                            self._kill_current_process_tree()
                            interrupted = True
                            return
                        time.sleep(0.15)

                cancel_watcher_thread = threading.Thread(target=_cancel_watcher, daemon=True)
                cancel_watcher_thread.start()
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
                        if realtime_iuid_enabled:
                            _process_realtime_stream_line(clean)
                        elif dcmtk_realtime_enabled:
                            m_file = DCMTK_SENDING_FILE_RE.search(clean)
                            if m_file:
                                dcmtk_current_file = m_file.group(1).strip()
                                self._log(
                                    f"[DCMTK_RT_PROGRESS] chunk={chunk_index}/{total_chunks} sending={dcmtk_current_file}"
                                )
                            m_bad = DCMTK_BAD_FILE_RE.search(clean)
                            if m_bad:
                                bad_file = m_bad.group(1).strip()
                                detail = m_bad.group(2).strip()
                                _write_dcmtk_realtime_row(
                                    file_path_s=bad_file,
                                    status_value="NON_DICOM",
                                    detail_value=detail,
                                )
                                if dcmtk_current_file == bad_file:
                                    dcmtk_current_file = ""
                            m_rsp = DCMTK_STORE_RSP_RE.search(clean)
                            if m_rsp and dcmtk_current_file:
                                detail = m_rsp.group(1).strip()
                                status = "SENT_OK" if "Success" in detail else "SEND_FAIL"
                                if ("Unknown Status: 0x110" in detail) and Path(dcmtk_current_file).name.upper() == "DICOMDIR":
                                    status = "UNSUPPORTED_DICOM_OBJECT"
                                _write_dcmtk_realtime_row(
                                    file_path_s=dcmtk_current_file,
                                    status_value=status,
                                    detail_value=detail,
                                )
                                dcmtk_current_file = ""
                        if show_output:
                            self._log_toolkit(clean)
                    if not interrupted:
                        self.current_proc.wait()
                        exit_code = self.current_proc.returncode if self.current_proc.returncode is not None else -1
                finally:
                    cancel_watcher_stop.set()
                    cancel_watcher_thread.join(timeout=1.2)
                    self.current_proc = None
            if interrupted:
                self._log(
                    f"[SEND_CANCELLED_IMMEDIATE] chunk={chunk_index}/{total_chunks} "
                    f"processed_items={item_cursor}/{total_items}"
                )
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
                    if fp in realtime_written_files:
                        continue
                    item_cursor += 1
                    src_iuid = ""
                    src_ts_uid = ""
                    src_ts_name = ""
                    uid_source = "NONE"
                    uid_from_filename = False
                    extract_status = ""
                    meta_err = ""
                    try:
                        src_iuid, src_ts_uid, src_ts_name, meta_err = self.driver.extract_metadata(self.cfg, file_path)
                    except Exception as ex:
                        meta_err = str(ex)
                    src_iuid = normalize_uid_candidate(src_iuid)
                    src_ts_uid = normalize_uid_candidate(src_ts_uid)
                    src_ts_name = normalize_uid_candidate(src_ts_name)
                    if src_iuid:
                        uid_source = "METADATA"

                    # Fallback: many datasets already embed SOPInstanceUID in filename.
                    if not src_iuid and looks_like_dicom_payload_file(file_path):
                        src_iuid = normalize_uid_candidate(Path(fp).name)
                        if src_iuid:
                            uid_source = "FILENAME_FALLBACK"
                            uid_from_filename = True
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
                        uid_source = "RQ_ORDER"
                    else:
                        src_iuid_prev = ""
                        uid_was_inferred = False

                    detail = (
                        f"dcm4che parse: iuid_mode={dcm4che_iuid_update_mode};"
                        f"rq_iuids={len(rq_iuid_set)};ok_iuids={len(ok_iuids)};"
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
                        detail += f";uid_source={uid_source}"
                        if src_iuid and src_iuid not in ok_iuid_set and src_iuid not in err_iuid_set and src_iuid not in rq_iuid_set:
                            src_iuid = ""
                            detail += ";uid_persisted=NO"
                            extract_status = "NO_MATCH_UID_UNCONFIRMED"
                        elif src_iuid:
                            detail += ";uid_persisted=YES"
                        if uid_from_filename and not src_iuid:
                            detail += ";uid_filename_fallback_rejected=YES"

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
                        if status == "SENT_UNKNOWN":
                            self._log(
                                f"[SEND_UID_SOURCE] file={fp} source={uid_source} "
                                f"persisted={'YES' if src_iuid else 'NO'} extract_status={extract_status}"
                            )
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
                    self.progress_callback(
                        item_cursor,
                        total_items,
                        attempt_chunk_no,
                        attempt_chunks_total,
                        chunk_index,
                        total_chunks,
                    )
                    _write_send_checkpoint("ITEM", fp)
            else:
                for file_path in batch_files:
                    fp = str(file_path)
                    if fp in dcmtk_written_files:
                        continue
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
                    if status == "SENT_UNKNOWN" and not detail:
                        detail = "parse_status=UNKNOWN;reason=no_match_in_output"
                    if status == "SENT_UNKNOWN" and detail:
                        self._log(f"[DCMTK_STATUS_DETAIL_ENRICHED] file={fp} reason={detail}")

                    if status == "SENT_OK":
                        sent_ok += 1
                    elif status in warning_statuses:
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
                    self.progress_callback(
                        item_cursor,
                        total_items,
                        attempt_chunk_no,
                        attempt_chunks_total,
                        chunk_index,
                        total_chunks,
                    )
                    _write_send_checkpoint("ITEM", fp)
            unit_cursor += len(batch_inputs)
            _write_send_checkpoint("CHUNK_SYNC")
            write_telemetry_event(
                events,
                run,
                "CHUNK_END",
                "Chunk concluido.",
                (
                    f"chunk_no={chunk_index};exit_code={exit_code};"
                    f"exec_mode={dcm4che_exec_mode if self.cfg.toolkit == 'dcm4che' else 'TOOLKIT_DEFAULT'};"
                    f"split_pos={split_pos};split_total={split_total};origin_chunk={original_chunk_no}"
                ),
            )
            self._log(
                f"[CHUNK_END] chunk={chunk_index}/{total_chunks} exit_code={exit_code} "
                f"processed_items={item_cursor}/{total_items} "
                f"exec_mode={dcm4che_exec_mode if self.cfg.toolkit == 'dcm4che' else 'TOOLKIT_DEFAULT'}"
            )

        aggregated_sent_ok = sent_ok
        aggregated_warn = warned
        aggregated_fail = failed
        aggregated_items_processed = item_cursor
        try:
            latest_by_file: dict[str, dict] = {}
            for row in read_csv_rows(send_results):
                fp = str(row.get("file_path", "")).strip()
                if fp in selected_file_set:
                    latest_by_file[fp] = row
            aggregated_items_processed = len(latest_by_file)
            aggregated_sent_ok = 0
            aggregated_warn = 0
            aggregated_fail = 0
            for fp, row in latest_by_file.items():
                status_v = str(row.get("send_status", "SENT_UNKNOWN")).strip() or "SENT_UNKNOWN"
                if status_v == "SENT_OK":
                    aggregated_sent_ok += 1
                elif status_v in warning_statuses:
                    aggregated_warn += 1
                else:
                    aggregated_fail += 1
        except Exception:
            pass

        final_status = "INTERRUPTED" if interrupted else (
            "PASS" if aggregated_fail == 0 and aggregated_warn == 0 else ("PASS_WITH_WARNINGS" if aggregated_fail == 0 else "FAIL")
        )
        send_duration_sec = round(max(time.monotonic() - send_start_ts, 0.0), 3)
        write_csv_row(
            send_summary,
            {
                "run_id": run,
                "toolkit": self.cfg.toolkit,
                "ts_mode_effective": ts_mode,
                "total_items": total_items,
                "items_processed": aggregated_items_processed,
                "sent_ok": aggregated_sent_ok,
                "warnings": aggregated_warn,
                "failed": aggregated_fail,
                "status": final_status,
                "send_duration_sec": send_duration_sec,
                "finished_at": now_br(),
            },
            ["run_id", "toolkit", "ts_mode_effective", "total_items", "items_processed", "sent_ok", "warnings", "failed", "status", "send_duration_sec", "finished_at"],
        )
        write_telemetry_event(
            events,
            run,
            "RUN_SEND_END",
            "Envio finalizado.",
            f"status={final_status};send_duration_sec={send_duration_sec}",
        )
        self._log(
            f"[SEND_END] status={final_status} processed_items={item_cursor}/{total_items} "
            f"duration={format_duration_sec(send_duration_sec)}"
        )
        self._log(
            f"[SEND_RESULT] ok={aggregated_sent_ok} warn={aggregated_warn} fail={aggregated_fail} status={final_status} "
            f"duration={format_duration_sec(send_duration_sec)}"
        )
        if aggregated_warn > 0:
            self._log(
                "[SEND_WARN_SUMMARY] "
                f"sent_unknown={warn_type_counts.get('SENT_UNKNOWN', 0)} "
                f"non_dicom={warn_type_counts.get('NON_DICOM', 0)} "
                f"unsupported={warn_type_counts.get('UNSUPPORTED_DICOM_OBJECT', 0)} "
                f"uid_empty_expected={warn_type_counts.get('UID_EMPTY_EXPECTED', 0)} "
                f"uid_empty_unexpected={warn_type_counts.get('UID_EMPTY_UNEXPECTED', 0)} "
                f"parse_exception_files={warn_type_counts.get('PARSE_EXCEPTION', 0)}"
            )
        return {"run_id": run, "status": final_status, "run_dir": str(run_dir), "send_duration_sec": send_duration_sec}
