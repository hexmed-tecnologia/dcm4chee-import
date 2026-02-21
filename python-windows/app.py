import csv
import json
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


SUCCESS_REGEX = re.compile(r"status=0H[\s\S]*?iuid=([\d\.]+)", re.IGNORECASE)
ERROR_REGEX = re.compile(r"status=[^0][A-F0-9]*H[\s\S]*?iuid=([\d\.]+)", re.IGNORECASE)


@dataclass
class AppConfig:
    dcm4che_bin_path: str = ""
    aet_destino: str = "HMD_IMPORTED"
    pacs_host: str = "192.168.1.70"
    pacs_port: int = 5555
    pacs_rest_host: str = "192.168.1.70:8080"
    runs_base_dir: str = ""
    nivel_log_minimo: str = "INFO"
    batch_size_default: int = 50


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def now_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def write_csv_row(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def find_first_tool_bin(base_dir: Path) -> str:
    for root, _, files in os.walk(base_dir):
        if "storescu.bat" in files:
            return str(Path(root))
    return ""


def get_leaf_dirs(root_path: Path) -> list[Path]:
    leaf_dirs: list[Path] = []
    stack = [root_path]
    while stack:
        current = stack.pop()
        try:
            subdirs = [p for p in current.iterdir() if p.is_dir()]
        except Exception:
            continue
        if not subdirs:
            leaf_dirs.append(current)
        else:
            stack.extend(subdirs)
    return sorted(leaf_dirs)


class SendWorkflow:
    def __init__(self, config: AppConfig, logger, cancel_event: threading.Event):
        self.config = config
        self.logger = logger
        self.cancel_event = cancel_event
        self.current_proc: subprocess.Popen | None = None
        self.success_set: set[str] = set()
        self.error_set: set[str] = set()

    def _log(self, msg: str) -> None:
        self.logger(msg)

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
            )
        except Exception:
            pass

    def _resolve_runs_base(self, script_dir: Path) -> Path:
        if self.config.runs_base_dir.strip():
            p = Path(self.config.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (script_dir / p).resolve()
        return (script_dir / "runs").resolve()

    def _append_new_iuids(self, text_blob: str, out_file: Path, target_set: set[str], regex: re.Pattern) -> int:
        new_count = 0
        for match in regex.findall(text_blob):
            iuid = match.strip()
            if iuid and iuid not in target_set:
                target_set.add(iuid)
                with out_file.open("a", encoding="utf-8") as f:
                    f.write(iuid + "\n")
                new_count += 1
        return new_count

    def run_send(self, exam_root: str, batch_size: int, run_id_resume: str = "", show_output: bool = True) -> dict:
        script_dir = Path(__file__).resolve().parent
        runs_base = self._resolve_runs_base(script_dir)
        runs_base.mkdir(parents=True, exist_ok=True)

        run_id = run_id_resume.strip() or now_run_id()
        run_dir = runs_base / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        storescu_bat = Path(self.config.dcm4che_bin_path) / "storescu.bat"
        if not storescu_bat.exists():
            raise RuntimeError(f"storescu.bat nao encontrado em: {storescu_bat}")

        exam_root_path = Path(exam_root).resolve()
        if not exam_root_path.exists():
            raise RuntimeError(f"Pasta de exames nao encontrada: {exam_root_path}")
        if batch_size < 1:
            raise RuntimeError("batch_size deve ser >= 1")

        # Artefatos do run
        log_file = run_dir / "storescu_execucao.log"
        success_file = run_dir / "sucesso_iuids.txt"
        error_file = run_dir / "erro_iuids.txt"
        checkpoint_file = run_dir / "send_checkpoint.json"
        manifest_file = run_dir / "manifest_folders.csv"
        folder_results_file = run_dir / "folder_results.csv"
        events_file = run_dir / "send_events.csv"
        summary_file = run_dir / "send_summary.csv"

        self._log(f"RUN_ID: {run_id}")
        self._log(f"Run dir: {run_dir}")
        self._log("Para cancelar: botao Cancelar (ou Ctrl+C no terminal da app).")

        checkpoint_completed: set[str] = set()
        resumed = False
        if checkpoint_file.exists():
            try:
                payload = json.loads(checkpoint_file.read_text(encoding="utf-8"))
                if payload.get("rootPath") == str(exam_root_path):
                    checkpoint_completed = set(payload.get("completedFolders", []))
                    resumed = True
            except Exception:
                resumed = False

        if not resumed:
            for p in [log_file, success_file, error_file, folder_results_file, events_file, summary_file]:
                if p.exists():
                    p.unlink()
                p.touch()
            if checkpoint_file.exists():
                checkpoint_file.unlink()
        else:
            if success_file.exists():
                for line in success_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                    line = line.strip()
                    if line:
                        self.success_set.add(line)
            if error_file.exists():
                for line in error_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                    line = line.strip()
                    if line:
                        self.error_set.add(line)

        write_csv_row(
            events_file,
            {
                "timestamp": now_iso(),
                "run_id": run_id,
                "level": "INFO",
                "event_type": "RUN_START",
                "batch": "",
                "folder": "",
                "message": "Inicio da execucao de envio.",
                "extra": f"run_dir={run_dir}",
            },
        )

        # Manifesto
        leaf_dirs = get_leaf_dirs(exam_root_path)
        if manifest_file.exists():
            manifest_file.unlink()
        for folder in leaf_dirs:
            file_count = 0
            try:
                file_count = sum(1 for p in folder.iterdir() if p.is_file())
            except Exception:
                file_count = 0
            write_csv_row(
                manifest_file,
                {
                    "run_id": run_id,
                    "folder_path": str(folder),
                    "file_count": file_count,
                    "discovered_at": now_iso(),
                },
            )

        pending = [d for d in leaf_dirs if str(d) not in checkpoint_completed]
        self._log(f"Pastas-fim encontradas: {len(leaf_dirs)}")
        self._log(f"Pastas pendentes: {len(pending)}")

        if not pending:
            write_csv_row(
                events_file,
                {
                    "timestamp": now_iso(),
                    "run_id": run_id,
                    "level": "INFO",
                    "event_type": "RUN_END",
                    "batch": "",
                    "folder": "",
                    "message": "Nada pendente para envio.",
                    "extra": "",
                },
            )
            return {"run_id": run_id, "run_dir": str(run_dir), "status": "PASS"}

        def save_checkpoint() -> None:
            payload = {
                "runId": run_id,
                "rootPath": str(exam_root_path),
                "batchSize": batch_size,
                "updatedAt": now_iso(),
                "completedFolders": sorted(list(checkpoint_completed)),
            }
            checkpoint_file.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

        total_batches = (len(pending) + batch_size - 1) // batch_size
        sent_count = 0
        skipped_empty = 0
        interrupted = False

        for offset in range(0, len(pending), batch_size):
            if self.cancel_event.is_set():
                interrupted = True
                break
            batch_no = (offset // batch_size) + 1
            batch = pending[offset : offset + batch_size]
            self._log(f"Batch {batch_no}/{total_batches} iniciado com {len(batch)} pastas.")

            write_csv_row(
                events_file,
                {
                    "timestamp": now_iso(),
                    "run_id": run_id,
                    "level": "INFO",
                    "event_type": "BATCH_START",
                    "batch": batch_no,
                    "folder": "",
                    "message": "Batch iniciado.",
                    "extra": f"batch_size={len(batch)}",
                },
            )

            for folder in batch:
                if self.cancel_event.is_set():
                    interrupted = True
                    break

                has_file = False
                try:
                    has_file = any(p.is_file() for p in folder.iterdir())
                except Exception:
                    has_file = False

                if not has_file:
                    skipped_empty += 1
                    checkpoint_completed.add(str(folder))
                    save_checkpoint()
                    self._log(f"[SKIPPED_EMPTY] {folder}")
                    write_csv_row(
                        folder_results_file,
                        {
                            "run_id": run_id,
                            "folder_path": str(folder),
                            "batch": batch_no,
                            "status": "SKIPPED_EMPTY",
                            "exit_code": "",
                            "iuids_sucesso_novos": 0,
                            "iuids_erro_novos": 0,
                            "processed_at": now_iso(),
                        },
                    )
                    continue

                self._log(f"[SEND] {folder}")
                write_csv_row(
                    events_file,
                    {
                        "timestamp": now_iso(),
                        "run_id": run_id,
                        "level": "INFO",
                        "event_type": "FOLDER_SEND_START",
                        "batch": batch_no,
                        "folder": str(folder),
                        "message": "Envio de pasta iniciado.",
                        "extra": "",
                    },
                )

                cmd = [
                    "cmd",
                    "/c",
                    str(storescu_bat),
                    "-c",
                    f"{self.config.aet_destino}@{self.config.pacs_host}:{self.config.pacs_port}",
                    str(folder),
                ]
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
                    )
                    try:
                        assert self.current_proc.stdout is not None
                        for line in self.current_proc.stdout:
                            if self.cancel_event.is_set():
                                self._kill_current_process_tree()
                                interrupted = True
                                break
                            lines.append(line.rstrip("\n"))
                            lf.write(line)
                            lf.flush()
                            if show_output:
                                self._log(line.rstrip("\n"))
                        if not interrupted:
                            self.current_proc.wait()
                            exit_code = self.current_proc.returncode if self.current_proc.returncode is not None else -1
                    finally:
                        self.current_proc = None

                if interrupted:
                    break

                blob = "\n".join(lines)
                new_ok = self._append_new_iuids(blob, success_file, self.success_set, SUCCESS_REGEX)
                new_err = self._append_new_iuids(blob, error_file, self.error_set, ERROR_REGEX)

                status = "DONE" if exit_code == 0 else "DONE_WITH_WARNINGS"
                if exit_code != 0:
                    self._log(f"[WARN] storescu exit code {exit_code} na pasta: {folder}")

                write_csv_row(
                    folder_results_file,
                    {
                        "run_id": run_id,
                        "folder_path": str(folder),
                        "batch": batch_no,
                        "status": status,
                        "exit_code": exit_code,
                        "iuids_sucesso_novos": new_ok,
                        "iuids_erro_novos": new_err,
                        "processed_at": now_iso(),
                    },
                )
                write_csv_row(
                    events_file,
                    {
                        "timestamp": now_iso(),
                        "run_id": run_id,
                        "level": "INFO",
                        "event_type": "FOLDER_SEND_END",
                        "batch": batch_no,
                        "folder": str(folder),
                        "message": "Envio de pasta concluido.",
                        "extra": f"status={status};exit_code={exit_code};new_success={new_ok};new_error={new_err}",
                    },
                )

                sent_count += 1
                checkpoint_completed.add(str(folder))
                save_checkpoint()

            if interrupted:
                break

            write_csv_row(
                events_file,
                {
                    "timestamp": now_iso(),
                    "run_id": run_id,
                    "level": "INFO",
                    "event_type": "BATCH_END",
                    "batch": batch_no,
                    "folder": "",
                    "message": "Batch concluido.",
                    "extra": "",
                },
            )

        if interrupted:
            run_status = "INTERRUPTED"
            write_csv_row(
                events_file,
                {
                    "timestamp": now_iso(),
                    "run_id": run_id,
                    "level": "WARN",
                    "event_type": "RUN_INTERRUPTED",
                    "batch": "",
                    "folder": "",
                    "message": "Execucao interrompida por cancelamento.",
                    "extra": "",
                },
            )
        else:
            run_status = "PASS_WITH_WARNINGS" if self.error_set else "PASS"
            write_csv_row(
                events_file,
                {
                    "timestamp": now_iso(),
                    "run_id": run_id,
                    "level": "INFO",
                    "event_type": "RUN_END",
                    "batch": "",
                    "folder": "",
                    "message": "Execucao finalizada.",
                    "extra": f"send_status={run_status}",
                },
            )

        write_csv_row(
            summary_file,
            {
                "run_id": run_id,
                "root_path": str(exam_root_path),
                "aet_destino": self.config.aet_destino,
                "batch_size": batch_size,
                "leaf_dirs_total": len(leaf_dirs),
                "folders_completed": len(checkpoint_completed),
                "folders_sent": sent_count,
                "folders_skipped_empty": skipped_empty,
                "success_iuids_total": len(self.success_set),
                "error_iuids_total": len(self.error_set),
                "send_status": run_status,
                "finished_at": now_iso(),
            },
        )

        self._log("--- Relatorio Final (SEND) ---")
        self._log(f"Run ID: {run_id}")
        self._log(f"Run dir: {run_dir}")
        self._log(f"Pastas-fim totais: {len(leaf_dirs)}")
        self._log(f"Pastas enviadas: {sent_count}")
        self._log(f"Pastas SKIPPED_EMPTY: {skipped_empty}")
        self._log(f"IUIDs sucesso: {len(self.success_set)}")
        self._log(f"IUIDs erro: {len(self.error_set)}")
        self._log(f"Status send: {run_status}")

        return {"run_id": run_id, "run_dir": str(run_dir), "status": run_status}


class ValidationWorkflow:
    def __init__(self, config: AppConfig, logger, cancel_event: threading.Event):
        self.config = config
        self.logger = logger
        self.cancel_event = cancel_event

    def _log(self, msg: str) -> None:
        self.logger(msg)

    def _resolve_runs_base(self, script_dir: Path) -> Path:
        if self.config.runs_base_dir.strip():
            p = Path(self.config.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (script_dir / p).resolve()
        return (script_dir / "runs").resolve()

    def run_validation(self, run_id: str) -> dict:
        run_id = run_id.strip()
        if not run_id:
            raise RuntimeError("run_id e obrigatorio para validacao.")

        script_dir = Path(__file__).resolve().parent
        runs_base = self._resolve_runs_base(script_dir)
        run_dir = runs_base / run_id
        if not run_dir.exists():
            raise RuntimeError(f"Run nao encontrado: {run_dir}")

        success_file = run_dir / "sucesso_iuids.txt"
        error_file = run_dir / "erro_iuids.txt"
        validation_file = run_dir / "validation_report.csv"
        events_file = run_dir / "validation_events.csv"
        reconciliation_file = run_dir / "reconciliation_report.csv"
        missing_file = run_dir / "nao_validados_iuids.txt"

        if not success_file.exists():
            raise RuntimeError(f"Arquivo nao encontrado: {success_file}")

        for p in [validation_file, events_file, reconciliation_file, missing_file]:
            if p.exists():
                p.unlink()

        iuids_success = sorted(set([x.strip() for x in success_file.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()]))
        iuids_error_send = []
        if error_file.exists():
            iuids_error_send = sorted(set([x.strip() for x in error_file.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()]))

        self._log(f"Iniciando validacao do run: {run_id}")
        self._log(f"IUIDs para validar: {len(iuids_success)}")

        write_csv_row(
            events_file,
            {
                "timestamp": now_iso(),
                "run_id": run_id,
                "level": "INFO",
                "event_type": "VALIDATION_START",
                "iuid": "",
                "message": "Inicio da validacao por IUID.",
                "extra": "",
            },
        )

        ok_count = 0
        not_found_count = 0
        api_error_count = 0
        not_validated: list[str] = []

        for iuid in iuids_success:
            if self.cancel_event.is_set():
                raise RuntimeError("Validacao cancelada pelo usuario.")

            url = f"http://{self.config.pacs_rest_host}/dcm4chee-arc/aets/{self.config.aet_destino}/rs/instances?SOPInstanceUID={iuid}"
            status = "NOT_FOUND"
            detail = ""

            try:
                req = urlrequest.Request(url, method="GET")
                with urlrequest.urlopen(req, timeout=20) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    data = json.loads(body) if body.strip() else []
                    if isinstance(data, list) and len(data) > 0:
                        status = "OK"
                        ok_count += 1
                    else:
                        status = "NOT_FOUND"
                        not_found_count += 1
                        not_validated.append(iuid)
            except urlerror.URLError as ex:
                status = "API_ERROR"
                detail = str(ex)
                api_error_count += 1
                not_validated.append(iuid)
            except Exception as ex:
                status = "API_ERROR"
                detail = str(ex)
                api_error_count += 1
                not_validated.append(iuid)

            write_csv_row(
                validation_file,
                {
                    "run_id": run_id,
                    "iuid": iuid,
                    "status": status,
                    "checked_at": now_iso(),
                    "api_detail": detail,
                },
            )
            write_csv_row(
                events_file,
                {
                    "timestamp": now_iso(),
                    "run_id": run_id,
                    "level": "INFO",
                    "event_type": "IUID_VALIDATED",
                    "iuid": iuid,
                    "message": "IUID validado.",
                    "extra": f"status={status}",
                },
            )

        if not_validated:
            missing_file.write_text("\n".join(sorted(set(not_validated))) + "\n", encoding="utf-8")

        total_success = len(iuids_success)
        total_send_errors = len(iuids_error_send)

        final_status = "PASS"
        reason = "Todos os IUIDs de sucesso foram encontrados no dcm4chee."
        if total_success > 0 and api_error_count == total_success:
            final_status = "FAIL"
            reason = "Falha total de consulta na API."
        elif not_found_count > 0 or api_error_count > 0 or total_send_errors > 0:
            final_status = "PASS_WITH_WARNINGS"
            reason = "Existem IUIDs nao validados e/ou erros no envio."

        write_csv_row(
            reconciliation_file,
            {
                "run_id": run_id,
                "total_iuids_sucesso": total_success,
                "total_iuids_ok": ok_count,
                "total_iuids_not_found": not_found_count,
                "total_iuids_api_error": api_error_count,
                "total_iuids_erro_send": total_send_errors,
                "final_status": final_status,
                "reason": reason,
                "generated_at": now_iso(),
            },
        )
        write_csv_row(
            events_file,
            {
                "timestamp": now_iso(),
                "run_id": run_id,
                "level": "INFO",
                "event_type": "VALIDATION_END",
                "iuid": "",
                "message": "Validacao finalizada.",
                "extra": f"final_status={final_status}",
            },
        )

        self._log("--- Relatorio Final (VALIDACAO) ---")
        self._log(f"Run ID: {run_id}")
        self._log(f"IUIDs sucesso (entrada): {total_success}")
        self._log(f"IUIDs OK: {ok_count}")
        self._log(f"IUIDs NOT_FOUND: {not_found_count}")
        self._log(f"IUIDs API_ERROR: {api_error_count}")
        self._log(f"IUIDs erro no send: {total_send_errors}")
        self._log(f"Status final: {final_status}")

        return {"run_id": run_id, "status": final_status, "run_dir": str(run_dir)}


class ConfigDialog(tk.Toplevel):
    def __init__(self, master, config: AppConfig, on_save, test_echo_callback):
        super().__init__(master)
        self.title("Configuracoes DCM4CHEE")
        self.resizable(False, False)
        self.config_obj = config
        self.on_save = on_save
        self.test_echo_callback = test_echo_callback

        self.var_bin = tk.StringVar(value=config.dcm4che_bin_path)
        self.var_aet = tk.StringVar(value=config.aet_destino)
        self.var_host = tk.StringVar(value=config.pacs_host)
        self.var_port = tk.StringVar(value=str(config.pacs_port))
        self.var_rest = tk.StringVar(value=config.pacs_rest_host)
        self.var_runs = tk.StringVar(value=config.runs_base_dir)
        self.var_batch = tk.StringVar(value=str(config.batch_size_default))
        self.var_log = tk.StringVar(value=config.nivel_log_minimo)

        frm = ttk.Frame(self, padding=12)
        frm.grid(sticky="nsew")
        self.columnconfigure(0, weight=1)

        self._row_entry(frm, 0, "dcm4che bin path", self.var_bin, browse=True)
        self._row_entry(frm, 1, "AET destino", self.var_aet)
        self._row_entry(frm, 2, "PACS host", self.var_host)
        self._row_entry(frm, 3, "PACS port", self.var_port)
        self._row_entry(frm, 4, "PACS REST host:porta", self.var_rest)
        self._row_entry(frm, 5, "Runs base dir (opcional)", self.var_runs, browse=True)
        self._row_entry(frm, 6, "Batch default", self.var_batch)

        ttk.Label(frm, text="Nivel log").grid(row=7, column=0, sticky="w", pady=3)
        cmb = ttk.Combobox(frm, textvariable=self.var_log, values=["DEBUG", "INFO", "WARN", "ERROR"], width=12, state="readonly")
        cmb.grid(row=7, column=1, sticky="w", pady=3)

        btns = ttk.Frame(frm)
        btns.grid(row=8, column=0, columnspan=3, pady=(12, 0), sticky="e")
        ttk.Button(btns, text="Testar Echo", command=self._test_echo).pack(side="left", padx=4)
        ttk.Button(btns, text="Salvar", command=self._save).pack(side="left", padx=4)
        ttk.Button(btns, text="Fechar", command=self.destroy).pack(side="left", padx=4)

    def _row_entry(self, parent, row, label, var, browse=False):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=3)
        ent = ttk.Entry(parent, textvariable=var, width=58)
        ent.grid(row=row, column=1, sticky="we", pady=3)
        if browse:
            ttk.Button(parent, text="...", width=3, command=lambda: self._browse(var)).grid(row=row, column=2, padx=(4, 0))

    def _browse(self, var):
        path = filedialog.askdirectory(parent=self)
        if path:
            var.set(path)

    def _test_echo(self):
        cfg = self._build_config_from_fields()
        ok, output = self.test_echo_callback(cfg)
        if ok:
            messagebox.showinfo("Echo OK", output or "C-ECHO executado com sucesso.", parent=self)
        else:
            messagebox.showerror("Echo Falhou", output or "Falha no teste de echo.", parent=self)

    def _build_config_from_fields(self) -> AppConfig:
        return AppConfig(
            dcm4che_bin_path=self.var_bin.get().strip(),
            aet_destino=self.var_aet.get().strip(),
            pacs_host=self.var_host.get().strip(),
            pacs_port=int(self.var_port.get().strip()),
            pacs_rest_host=self.var_rest.get().strip(),
            runs_base_dir=self.var_runs.get().strip(),
            nivel_log_minimo=self.var_log.get().strip(),
            batch_size_default=int(self.var_batch.get().strip()),
        )

    def _save(self):
        try:
            cfg = self._build_config_from_fields()
        except Exception as ex:
            messagebox.showerror("Erro", f"Configuracao invalida: {ex}", parent=self)
            return
        self.on_save(cfg)
        messagebox.showinfo("OK", "Configuracoes salvas.", parent=self)
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("DICOM Sender/Validator MVP")
        self.geometry("1100x700")

        self.base_dir = Path(__file__).resolve().parent
        self.config_file = self.base_dir / "app_config.json"
        self.config_obj = self._load_config()

        self.queue: queue.Queue = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()

        self._build_menu()
        self._build_ui()
        self._poll_queue()

    def _load_config(self) -> AppConfig:
        default_bin = find_first_tool_bin(self.base_dir)
        cfg = AppConfig(dcm4che_bin_path=default_bin)
        if self.config_file.exists():
            try:
                raw = json.loads(self.config_file.read_text(encoding="utf-8"))
                cfg = AppConfig(**{**asdict(cfg), **raw})
            except Exception:
                pass
        return cfg

    def _save_config(self, cfg: AppConfig):
        self.config_obj = cfg
        self.config_file.write_text(json.dumps(asdict(cfg), ensure_ascii=True, indent=2), encoding="utf-8")
        self.var_batch_size.set(str(cfg.batch_size_default))
        self._log_send("Configuracoes atualizadas.")
        self._refresh_run_list()

    def _build_menu(self):
        menu = tk.Menu(self)
        self.config(menu=menu)
        m_cfg = tk.Menu(menu, tearoff=0)
        m_cfg.add_command(label="Configuracoes DCM4CHEE", command=self._open_config_dialog)
        m_cfg.add_command(label="Atualizar lista de runs", command=self._refresh_run_list)
        menu.add_cascade(label="Configuracao", menu=m_cfg)

    def _build_ui(self):
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True)

        self.tab_send = ttk.Frame(notebook)
        self.tab_val = ttk.Frame(notebook)
        self.tab_runs = ttk.Frame(notebook)
        notebook.add(self.tab_send, text="Send")
        notebook.add(self.tab_val, text="Validacao")
        notebook.add(self.tab_runs, text="Runs")

        self._build_send_tab()
        self._build_validation_tab()
        self._build_runs_tab()

    def _build_send_tab(self):
        top = ttk.Frame(self.tab_send, padding=10)
        top.pack(fill="x")

        self.var_exam_root = tk.StringVar()
        self.var_batch_size = tk.StringVar(value=str(self.config_obj.batch_size_default))
        self.var_resume_run = tk.StringVar()
        self.var_show_output = tk.BooleanVar(value=True)

        ttk.Label(top, text="Pasta exames").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.var_exam_root, width=90).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top, text="...", width=3, command=self._browse_exam_root).grid(row=0, column=2)

        ttk.Label(top, text="Batch size").grid(row=1, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.var_batch_size, width=10).grid(row=1, column=1, sticky="w", padx=6)
        ttk.Checkbutton(top, text="Mostrar output em tempo real", variable=self.var_show_output).grid(row=1, column=1, padx=90, sticky="w")

        ttk.Label(top, text="Run ID para retomar (opcional)").grid(row=2, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.var_resume_run, width=35).grid(row=2, column=1, sticky="w", padx=6)

        btns = ttk.Frame(top)
        btns.grid(row=3, column=0, columnspan=3, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Iniciar Send", command=self._start_send).pack(side="left", padx=4)
        ttk.Button(btns, text="Cancelar", command=self._cancel_current_job).pack(side="left", padx=4)

        log_frame = ttk.Frame(self.tab_send, padding=(10, 0, 10, 10))
        log_frame.pack(fill="both", expand=True)
        self.txt_send = tk.Text(log_frame, wrap="none")
        self.txt_send.pack(side="left", fill="both", expand=True)
        y = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_send.yview)
        y.pack(side="right", fill="y")
        self.txt_send.configure(yscrollcommand=y.set)

    def _build_validation_tab(self):
        top = ttk.Frame(self.tab_val, padding=10)
        top.pack(fill="x")

        self.var_validation_run = tk.StringVar()
        ttk.Label(top, text="Run ID").grid(row=0, column=0, sticky="w")
        self.cmb_runs = ttk.Combobox(top, textvariable=self.var_validation_run, width=40)
        self.cmb_runs.grid(row=0, column=1, sticky="w", padx=6)
        ttk.Button(top, text="Atualizar", command=self._refresh_run_list).grid(row=0, column=2, padx=4)
        ttk.Button(top, text="Validar Run", command=self._start_validation).grid(row=0, column=3, padx=4)
        ttk.Button(top, text="Cancelar", command=self._cancel_current_job).grid(row=0, column=4, padx=4)

        log_frame = ttk.Frame(self.tab_val, padding=(10, 0, 10, 10))
        log_frame.pack(fill="both", expand=True)
        self.txt_val = tk.Text(log_frame, wrap="none")
        self.txt_val.pack(side="left", fill="both", expand=True)
        y = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_val.yview)
        y.pack(side="right", fill="y")
        self.txt_val.configure(yscrollcommand=y.set)

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
        self.cmb_runs["values"] = runs
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

    def _test_echo(self, cfg: AppConfig) -> tuple[bool, str]:
        storescu = Path(cfg.dcm4che_bin_path) / "storescu.bat"
        if not storescu.exists():
            return False, f"storescu.bat nao encontrado em: {storescu}"
        # dcm4che storescu envia C-ECHO quando nao ha arquivo DICOM de entrada.
        cmd = ["cmd", "/c", str(storescu), "-c", f"{cfg.aet_destino}@{cfg.pacs_host}:{cfg.pacs_port}"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            out = out.strip()
            if proc.returncode == 0:
                return True, out
            return False, out or f"Echo falhou com exit code {proc.returncode}"
        except Exception as ex:
            return False, str(ex)

    def _browse_exam_root(self):
        path = filedialog.askdirectory(parent=self)
        if path:
            self.var_exam_root.set(path)

    def _worker_busy(self) -> bool:
        return self.worker_thread is not None and self.worker_thread.is_alive()

    def _start_send(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ja existe um processo em execucao.")
            return
        exam_root = self.var_exam_root.get().strip()
        if not exam_root:
            messagebox.showerror("Erro", "Informe a pasta de exames.")
            return
        try:
            batch_size = int(self.var_batch_size.get().strip())
        except Exception:
            messagebox.showerror("Erro", "Batch size invalido.")
            return

        self.cancel_event.clear()
        self._log_send("Iniciando SEND...")
        run_id_resume = self.var_resume_run.get().strip()
        show_output = bool(self.var_show_output.get())

        def task():
            try:
                wf = SendWorkflow(self.config_obj, lambda msg: self.queue.put(("send_log", msg)), self.cancel_event)
                result = wf.run_send(exam_root=exam_root, batch_size=batch_size, run_id_resume=run_id_resume, show_output=show_output)
                self.queue.put(("send_done", result))
            except Exception as ex:
                self.queue.put(("send_error", str(ex)))

        self.worker_thread = threading.Thread(target=task, daemon=True)
        self.worker_thread.start()

    def _start_validation(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ja existe um processo em execucao.")
            return
        run_id = self.var_validation_run.get().strip()
        if not run_id:
            messagebox.showerror("Erro", "Informe o run_id para validacao.")
            return

        self.cancel_event.clear()
        self._log_val("Iniciando VALIDACAO...")

        def task():
            try:
                wf = ValidationWorkflow(self.config_obj, lambda msg: self.queue.put(("val_log", msg)), self.cancel_event)
                result = wf.run_validation(run_id=run_id)
                self.queue.put(("val_done", result))
            except Exception as ex:
                self.queue.put(("val_error", str(ex)))

        self.worker_thread = threading.Thread(target=task, daemon=True)
        self.worker_thread.start()

    def _cancel_current_job(self):
        if not self._worker_busy():
            return
        self.cancel_event.set()
        self._log_send("Cancelamento solicitado...")
        self._log_val("Cancelamento solicitado...")

    def _log_send(self, text: str):
        self.txt_send.insert("end", text + "\n")
        self.txt_send.see("end")

    def _log_val(self, text: str):
        self.txt_val.insert("end", text + "\n")
        self.txt_val.see("end")

    def _poll_queue(self):
        try:
            while True:
                event, payload = self.queue.get_nowait()
                if event == "send_log":
                    self._log_send(payload)
                elif event == "val_log":
                    self._log_val(payload)
                elif event == "send_done":
                    self._log_send(f"SEND finalizado. Run ID: {payload.get('run_id')} | Status: {payload.get('status')}")
                    self._refresh_run_list()
                elif event == "val_done":
                    self._log_val(f"VALIDACAO finalizada. Run ID: {payload.get('run_id')} | Status: {payload.get('status')}")
                    self._refresh_run_list()
                elif event == "send_error":
                    self._log_send(f"[ERRO] {payload}")
                    messagebox.showerror("Erro no SEND", payload)
                elif event == "val_error":
                    self._log_val(f"[ERRO] {payload}")
                    messagebox.showerror("Erro na VALIDACAO", payload)
        except queue.Empty:
            pass
        self.after(120, self._poll_queue)


if __name__ == "__main__":
    app = App()
    app.mainloop()
