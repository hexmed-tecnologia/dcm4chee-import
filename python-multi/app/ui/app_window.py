import json
import os
import queue
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import asdict
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from app.config.settings import AppConfig
from app.domain.constants import APP_DISPLAY_NAME
from app.infra.run_artifacts import read_csv_rows, resolve_run_artifact_path
from app.integrations.toolkit_drivers import apply_internal_toolkit_paths, find_toolkit_bin, get_driver
from app.shared.utils import (
    format_duration_sec,
    hidden_process_kwargs,
    normalize_dcm4che_iuid_update_mode,
    normalize_dcm4che_send_mode,
    read_app_version,
    WorkflowCancelled,
)
from app.ui.config_dialog import ConfigDialog
from app.workflows.analyze import AnalyzeWorkflow
from app.workflows.send import SendWorkflow
from app.workflows.validation import ValidationWorkflow


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.base_dir = Path(__file__).resolve().parent.parent.parent
        self.app_version = read_app_version(self.base_dir)
        self.title(f"{APP_DISPLAY_NAME} - {self.app_version}")
        self.geometry("1180x760")
        self.config_file = self.base_dir / "app_config.json"
        self.config_obj = self._load_config()
        self.queue: queue.Queue = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()

        self.progress_items_var = tk.StringVar(value="enviando item 0 de 0")
        self.progress_chunks_var = tk.StringVar(value="batch chunk 0 de 0 | retomada #0")
        self.analysis_progress_var = tk.StringVar(value="progresso analise: aguardando")
        self.log_filter_options = ["Todos", "Sistema", "Warnings + Erros"]
        self.var_log_filter_an = tk.StringVar(value="Todos")
        self.var_log_filter_send = tk.StringVar(value="Todos")
        self.var_log_filter_val = tk.StringVar(value="Todos")
        self._max_log_buffer_lines = 6000
        self._log_refresh_batch_size = 300
        self._log_filter_debounce_ms = 180
        self._log_buffers: dict[str, list[tuple[str, str, str]]] = {"an": [], "send": [], "val": []}
        self._log_buffer_versions: dict[str, int] = {"an": 0, "send": 0, "val": 0}
        self._log_widgets: dict[str, tk.Text] = {}
        self._log_refresh_tokens: dict[str, int] = {"an": 0, "send": 0, "val": 0}
        self._log_refresh_after_ids: dict[str, str | None] = {"an": None, "send": None, "val": None}
        self._log_render_after_ids: dict[str, str | None] = {"an": None, "send": None, "val": None}
        self._log_render_state: dict[str, dict] = {}
        self._log_filter_cache: dict[tuple, list[tuple[str, str, str]]] = {}
        self.activity_status_an = tk.StringVar(value="ocioso")
        self.activity_status_send = tk.StringVar(value="ocioso")
        self.activity_status_val = tk.StringVar(value="ocioso")
        self._activity_context = ""
        self._activity_running = False
        self._activity_bars: list[ttk.Progressbar] = []
        self._batch_size_max_cmd_limit: int | None = None
        self._batch_size_max_cmd_source = ""
        self._batch_size_trace_guard = False

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
        cfg.dcm4che_iuid_update_mode = normalize_dcm4che_iuid_update_mode(cfg.dcm4che_iuid_update_mode)
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
        self._batch_size_max_cmd_limit = None
        self._batch_size_max_cmd_source = ""
        self._log_an(
            f"[CFG_SAVE] toolkit={cfg.toolkit} aet_origem={cfg.aet_origem} aet_destino={cfg.aet_destino} "
            f"pacs_dicom={cfg.pacs_host}:{cfg.pacs_port} pacs_rest={cfg.pacs_rest_host} "
            f"batch={cfg.batch_size_default} restrict_extensions={'ON' if cfg.restrict_extensions else 'OFF'} "
            f"include_no_extension={'ON' if cfg.include_no_extension else 'OFF'} "
            f"collect_size_bytes={'ON' if cfg.collect_size_bytes else 'OFF'} "
            f"dcm4che_send_mode={cfg.dcm4che_send_mode} "
            f"dcm4che_iuid_update_mode={cfg.dcm4che_iuid_update_mode}"
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
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True)
        self.tab_an = ttk.Frame(self.nb)
        self.tab_send = ttk.Frame(self.nb)
        self.tab_val = ttk.Frame(self.nb)
        self.tab_runs = ttk.Frame(self.nb)
        self.nb.add(self.tab_an, text="Analise")
        self.nb.add(self.tab_send, text="Send")
        self.nb.add(self.tab_val, text="Validacao")
        self.nb.add(self.tab_runs, text="Runs")
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
        self.var_batch_size.trace_add("write", self._on_batch_size_changed)
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
        cmb_filter_an.bind("<<ComboboxSelected>>", lambda _e: self._on_log_filter_changed("an"))

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
        self.cmb_send_runs.bind("<<ComboboxSelected>>", lambda _e: self._on_send_run_selected())
        ttk.Button(top, text="Atualizar", command=self._refresh_run_list).grid(row=0, column=2, padx=4)
        ttk.Button(top, text="Novo run", command=self._new_run_from_send).grid(row=0, column=3, padx=4)
        ttk.Checkbutton(
            top,
            text="Exibir mensagens internas do sistema",
            variable=self.var_show_send_internal,
            command=lambda: self._on_log_filter_changed("send"),
        ).grid(row=1, column=1, sticky="w", padx=6)
        ttk.Checkbutton(
            top,
            text="Exibir output bruto da toolkit (tempo real)",
            variable=self.var_show_output,
            command=lambda: self._on_log_filter_changed("send"),
        ).grid(row=2, column=1, sticky="w", padx=6)
        btns = ttk.Frame(top)
        btns.grid(row=3, column=0, columnspan=5, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Iniciar Send", command=self._start_send).pack(side="left", padx=4)
        ttk.Button(btns, text="Cancelar", command=self._cancel_current_job).pack(side="left", padx=4)

        status_row = ttk.Frame(self.tab_send, padding=(10, 0, 10, 8))
        status_row.pack(fill="x")

        prog = ttk.LabelFrame(status_row, text="Progresso", padding=10)
        prog.pack(side="left", fill="x", expand=True, padx=(0, 8))
        ttk.Label(prog, textvariable=self.progress_items_var).pack(anchor="w")
        ttk.Label(prog, textvariable=self.progress_chunks_var).pack(anchor="w")

        side_panel = ttk.LabelFrame(status_row, text="Atividade e Filtros", padding=10)
        side_panel.pack(side="left", fill="y")
        activity = ttk.Frame(side_panel)
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

        filter_bar = ttk.Frame(side_panel)
        filter_bar.pack(fill="x", pady=(8, 0))
        ttk.Label(filter_bar, text="Filtro de log (tela)").pack(side="left")
        cmb_filter_send = ttk.Combobox(
            filter_bar,
            textvariable=self.var_log_filter_send,
            values=self.log_filter_options,
            width=18,
            state="readonly",
        )
        cmb_filter_send.pack(side="left", padx=(8, 0))
        cmb_filter_send.bind("<<ComboboxSelected>>", lambda _e: self._on_log_filter_changed("send"))

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
        cmb_filter_val.bind("<<ComboboxSelected>>", lambda _e: self._on_log_filter_changed("val"))
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
        self._on_send_run_selected()

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

    def _safe_int(self, value, default: int = 0) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return default

    def _set_batch_size_ui_value(self, value: int) -> None:
        self._batch_size_trace_guard = True
        try:
            self.var_batch_size.set(str(value))
        finally:
            self._batch_size_trace_guard = False

    def _on_batch_size_changed(self, *_args) -> None:
        if self._batch_size_trace_guard:
            return
        limit = self._batch_size_max_cmd_limit
        if limit is None or limit <= 0:
            return
        raw = (self.var_batch_size.get() or "").strip()
        if not raw:
            return
        try:
            value = int(raw)
        except Exception:
            return
        if value > limit:
            self._set_batch_size_ui_value(limit)
            self._log_an(f"[BATCH_LIMIT_GUARD] batch={value} excede limite={limit}; ajustado automaticamente.")

    def _read_run_analysis_summary(self, run_id: str) -> dict:
        rid = (run_id or "").strip()
        if not rid:
            return {}
        run_dir = self._runs_base() / rid
        if not run_dir.exists():
            return {}
        summary = resolve_run_artifact_path(run_dir, "analysis_summary.csv", for_write=False)
        if not summary.exists():
            return {}
        rows = read_csv_rows(summary)
        return rows[-1] if rows else {}

    def _get_run_batch_max_cmd(self, run_id: str) -> tuple[int | None, str]:
        row = self._read_run_analysis_summary(run_id)
        if not row:
            return None, ""
        source = str(row.get("batch_max_cmd_source", "")).strip()
        limit_raw = str(row.get("batch_max_cmd", "")).strip()
        if source == "DCM4CHE_CMD_LIMIT" and limit_raw != "":
            return self._safe_int(limit_raw, -1), source
        return None, source

    def _apply_batch_limit_for_run(self, run_id: str, *, notify: bool, auto_set: bool) -> None:
        if (self.config_obj.toolkit or "").strip().lower() != "dcm4che":
            self._batch_size_max_cmd_limit = None
            self._batch_size_max_cmd_source = ""
            return
        limit, source = self._get_run_batch_max_cmd(run_id)
        if limit is None:
            self._batch_size_max_cmd_limit = None
            self._batch_size_max_cmd_source = ""
            return
        if limit <= 0:
            self._batch_size_max_cmd_limit = limit
            self._batch_size_max_cmd_source = source
            self._log_an(
                f"[BATCH_AUTO_MAX] run_id={run_id} source={source} limite_invalido={limit} "
                "envio sera bloqueado ate nova analise."
            )
            return
        prev = self._safe_int(self.var_batch_size.get(), limit)
        changed = (
            self._batch_size_max_cmd_limit != limit
            or self._batch_size_max_cmd_source != source
            or (auto_set and prev != limit)
        )
        self._batch_size_max_cmd_limit = limit
        self._batch_size_max_cmd_source = source
        if auto_set:
            self._set_batch_size_ui_value(limit)
        if changed:
            self._log_an(
                f"[BATCH_AUTO_MAX] run_id={run_id} source={source} batch_max_cmd={limit} "
                f"batch_anterior={prev} batch_aplicado={limit if auto_set else prev}"
            )
        if notify:
            messagebox.showinfo(
                "Batch ajustado automaticamente",
                "Limite maximo de batch para comando do dcm4che calculado com base nos caminhos analisados.\n\n"
                f"Run ID: {run_id}\n"
                f"Batch maximo seguro: {limit}\n\n"
                "Voce pode reduzir esse valor, mas aumentar acima do limite nao e permitido.",
                parent=self,
            )

    def _on_send_run_selected(self) -> None:
        self._apply_batch_limit_for_run(self.var_send_run.get().strip(), notify=False, auto_set=True)

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
        if (self.config_obj.toolkit or "").strip().lower() == "dcm4che":
            self._apply_batch_limit_for_run(run_id, notify=False, auto_set=False)
            limit = self._batch_size_max_cmd_limit
            if limit is not None and batch > limit:
                self._set_batch_size_ui_value(limit)
                self._log_send(
                    f"[BATCH_LIMIT_GUARD] run_id={run_id} batch_solicitado={batch} excede limite={limit}; "
                    f"ajustado automaticamente."
                )
                messagebox.showwarning(
                    "Batch acima do limite",
                    "O batch informado excede o limite maximo seguro do comando dcm4che.\n\n"
                    f"Run ID: {run_id}\n"
                    f"Limite maximo seguro: {limit}\n\n"
                    "O valor foi ajustado automaticamente.",
                    parent=self,
                )
                batch = limit
            if limit is not None and limit <= 0:
                messagebox.showerror(
                    "Limite de batch invalido",
                    "Nao foi possivel definir um batch seguro para este run no dcm4che.\n"
                    "Revise os caminhos dos arquivos e execute a analise novamente.",
                    parent=self,
                )
                return
        self.cancel_event.clear()
        self._log_send("Iniciando envio...")
        self.progress_items_var.set("enviando item 0 de 0")
        self.progress_chunks_var.set("batch chunk 0 de 0 | retomada #0")
        show_output = bool(self.var_show_output.get())
        self._set_activity_context("Send")
        self._set_activity_running(True)

        def progress(
            items_done,
            items_total,
            attempt_chunk_no,
            attempt_chunk_total,
            tech_chunk_no,
            tech_chunk_total,
        ):
            self.queue.put(
                (
                    "send_progress",
                    (
                        items_done,
                        items_total,
                        attempt_chunk_no,
                        attempt_chunk_total,
                        tech_chunk_no,
                        tech_chunk_total,
                    ),
                )
            )

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

    def _new_run_from_send(self):
        if self._worker_busy():
            messagebox.showwarning("Em execucao", "Ha um processo em execucao. Aguarde ou cancele antes de iniciar novo run.")
            return
        self.var_send_run.set("")
        self.var_run_id.set("")
        self._batch_size_max_cmd_limit = None
        self._batch_size_max_cmd_source = ""
        self._log_send("[RUN_NEW] Novo run solicitado na aba Send; selecao atual de run foi limpa.")
        self._log_an("[RUN_NEW] Pronto para nova analise. Informe pasta e execute 'Analisar Pasta'.")
        try:
            self.nb.select(self.tab_an)
        except Exception:
            pass

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
        self._log_send("[SEND_CANCEL_REQUEST] Cancelamento solicitado...")
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

    def _line_matches_filter_values(
        self,
        panel: str,
        tag: str,
        source: str,
        mode: str,
        show_send_internal: bool,
        show_send_toolkit: bool,
    ) -> bool:
        if panel == "send":
            if source == "internal" and not show_send_internal:
                return False
            if source == "toolkit" and not show_send_toolkit:
                return False
        if mode == "Todos":
            return True
        if mode == "Sistema":
            return tag == "log_system"
        if mode == "Warnings + Erros":
            return tag in ["log_warn", "log_error"]
        return True

    def _line_matches_filter(self, panel: str, tag: str, source: str) -> bool:
        return self._line_matches_filter_values(
            panel,
            tag,
            source,
            self._log_filter_mode(panel),
            bool(self.var_show_send_internal.get()),
            bool(self.var_show_output.get()),
        )

    def _append_widget_line(
        self,
        widget: tk.Text,
        text: str,
        tag: str,
        *,
        enforce_limit: bool = True,
        auto_scroll: bool = True,
    ) -> None:
        if tag:
            widget.insert("end", text + "\n", tag)
        else:
            widget.insert("end", text + "\n")
        if enforce_limit:
            line_count = int(widget.index("end-1c").split(".")[0])
            if line_count > self._max_log_buffer_lines:
                excess = line_count - self._max_log_buffer_lines
                widget.delete("1.0", f"{excess + 1}.0")
        if auto_scroll:
            widget.see("end")

    def _build_log_view_cache_key(self, panel: str, mode: str, show_send_internal: bool, show_send_toolkit: bool) -> tuple:
        return (
            panel,
            mode,
            show_send_internal if panel == "send" else True,
            show_send_toolkit if panel == "send" else True,
            self._log_buffer_versions.get(panel, 0),
        )

    def _emit_log_refresh_marker(self, panel: str, message: str) -> None:
        print(message)

    def _on_log_filter_changed(self, panel: str) -> None:
        mode = self._log_filter_mode(panel)
        self._emit_log_refresh_marker(panel, f"[LOG_FILTER_CHANGE] panel={panel} mode={mode}")
        self._schedule_log_refresh(panel, debounce_ms=self._log_filter_debounce_ms)

    def _schedule_log_refresh(self, panel: str, debounce_ms: int) -> None:
        prev_after = self._log_refresh_after_ids.get(panel)
        if prev_after:
            try:
                self.after_cancel(prev_after)
            except Exception:
                pass
        self._log_refresh_after_ids[panel] = self.after(
            max(debounce_ms, 0),
            lambda: self._start_log_refresh(panel),
        )

    def _start_log_refresh(self, panel: str) -> None:
        widget = self._log_widgets.get(panel)
        if widget is None:
            return
        self._log_refresh_after_ids[panel] = None
        token = self._log_refresh_tokens.get(panel, 0) + 1
        self._log_refresh_tokens[panel] = token
        mode = self._log_filter_mode(panel)
        show_send_internal = bool(self.var_show_send_internal.get())
        show_send_toolkit = bool(self.var_show_output.get())
        cache_key = self._build_log_view_cache_key(panel, mode, show_send_internal, show_send_toolkit)
        cached = self._log_filter_cache.get(cache_key)
        if cached is not None:
            self._emit_log_refresh_marker(
                panel,
                f"[LOG_REFRESH_START] panel={panel} token={token} mode={mode} "
                f"buffer={len(self._log_buffers.get(panel, []))} source=CACHE",
            )
            self._begin_log_refresh_render(panel, token, list(cached), build_duration_ms=0, source="CACHE")
            return

        snapshot = list(self._log_buffers.get(panel, []))
        self._emit_log_refresh_marker(
            panel,
            f"[LOG_REFRESH_START] panel={panel} token={token} mode={mode} "
            f"buffer={len(snapshot)} source=WORKER",
        )

        worker = threading.Thread(
            target=self._compute_log_refresh_snapshot,
            args=(panel, token, snapshot, mode, show_send_internal, show_send_toolkit, cache_key),
            daemon=True,
        )
        worker.start()

    def _compute_log_refresh_snapshot(
        self,
        panel: str,
        token: int,
        snapshot: list[tuple[str, str, str]],
        mode: str,
        show_send_internal: bool,
        show_send_toolkit: bool,
        cache_key: tuple,
    ) -> None:
        start = time.monotonic()
        filtered: list[tuple[str, str, str]] = []
        for text, tag, source in snapshot:
            if self._line_matches_filter_values(panel, tag, source, mode, show_send_internal, show_send_toolkit):
                filtered.append((text, tag, source))
        elapsed_ms = int((time.monotonic() - start) * 1000)
        self.queue.put(
            (
                "log_refresh_ready",
                {
                    "panel": panel,
                    "token": token,
                    "lines": filtered,
                    "cache_key": cache_key,
                    "build_duration_ms": elapsed_ms,
                },
            )
        )

    def _begin_log_refresh_render(
        self,
        panel: str,
        token: int,
        filtered_lines: list[tuple[str, str, str]],
        *,
        build_duration_ms: int,
        source: str,
    ) -> None:
        widget = self._log_widgets.get(panel)
        if widget is None:
            return
        prev_render = self._log_render_after_ids.get(panel)
        if prev_render:
            try:
                self.after_cancel(prev_render)
            except Exception:
                pass
            self._log_render_after_ids[panel] = None
        self._log_render_state[panel] = {
            "token": token,
            "lines": filtered_lines,
            "index": 0,
            "inserted": 0,
            "started_at": time.monotonic(),
            "build_duration_ms": build_duration_ms,
            "source": source,
        }
        widget.delete("1.0", "end")
        self._log_render_after_ids[panel] = self.after(0, lambda: self._render_log_refresh_batch(panel))

    def _render_log_refresh_batch(self, panel: str) -> None:
        state = self._log_render_state.get(panel)
        widget = self._log_widgets.get(panel)
        if not state or widget is None:
            return
        token = state.get("token", 0)
        if token != self._log_refresh_tokens.get(panel, 0):
            self._emit_log_refresh_marker(
                panel,
                f"[LOG_REFRESH_CANCELLED] panel={panel} stale_token={token}",
            )
            self._log_render_state.pop(panel, None)
            self._log_render_after_ids[panel] = None
            return

        lines = state.get("lines", [])
        idx = int(state.get("index", 0))
        next_idx = min(idx + self._log_refresh_batch_size, len(lines))
        batch = lines[idx:next_idx]
        for text, tag, _source in batch:
            self._append_widget_line(widget, text, tag, enforce_limit=False, auto_scroll=False)
        state["index"] = next_idx
        state["inserted"] = int(state.get("inserted", 0)) + len(batch)

        if next_idx < len(lines):
            remaining = len(lines) - next_idx
            self._emit_log_refresh_marker(
                panel,
                f"[LOG_REFRESH_BATCH] panel={panel} token={token} inserted={state.get('inserted', 0)} "
                f"remaining={remaining}",
            )
            self._log_render_after_ids[panel] = self.after(1, lambda: self._render_log_refresh_batch(panel))
            return

        # finalize view housekeeping once per refresh
        line_count = int(widget.index("end-1c").split(".")[0])
        if line_count > self._max_log_buffer_lines:
            excess = line_count - self._max_log_buffer_lines
            widget.delete("1.0", f"{excess + 1}.0")
        widget.see("end")
        elapsed_ms = int((time.monotonic() - float(state.get("started_at", time.monotonic()))) * 1000)
        self._emit_log_refresh_marker(
            panel,
            f"[LOG_REFRESH_END] panel={panel} token={token} inserted={state.get('inserted', 0)} "
            f"build_ms={state.get('build_duration_ms', 0)} render_ms={elapsed_ms} source={state.get('source', 'UNK')}",
        )
        self._log_render_state.pop(panel, None)
        self._log_render_after_ids[panel] = None

    def _refresh_log_view(self, panel: str) -> None:
        self._schedule_log_refresh(panel, debounce_ms=0)

    def _append_log_line(self, panel: str, text: str, source: str = "internal") -> None:
        tag = self._classify_log_tag(text)
        buf = self._log_buffers.setdefault(panel, [])
        buf.append((text, tag, source))
        if len(buf) > self._max_log_buffer_lines:
            removed = len(buf) - self._max_log_buffer_lines
            del buf[:removed]
            print(f"[LOG_BUFFER_TRIM] panel={panel} removed={removed} max={self._max_log_buffer_lines}")
        self._log_buffer_versions[panel] = self._log_buffer_versions.get(panel, 0) + 1
        if len(self._log_filter_cache) > 32:
            latest_versions = self._log_buffer_versions.copy()
            self._log_filter_cache = {
                key: val
                for key, val in self._log_filter_cache.items()
                if latest_versions.get(key[0], -1) == key[4]
            }
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
                elif event == "log_refresh_ready":
                    panel = payload.get("panel", "")
                    token = int(payload.get("token", 0))
                    if token != self._log_refresh_tokens.get(panel, 0):
                        self._emit_log_refresh_marker(
                            panel,
                            f"[LOG_REFRESH_CANCELLED] panel={panel} stale_token={token} reason=worker_result",
                        )
                        continue
                    cache_key = payload.get("cache_key")
                    if isinstance(cache_key, tuple):
                        self._log_filter_cache[cache_key] = list(payload.get("lines", []))
                    self._begin_log_refresh_render(
                        panel,
                        token,
                        list(payload.get("lines", [])),
                        build_duration_ms=int(payload.get("build_duration_ms", 0)),
                        source="WORKER",
                    )
                elif event == "an_done":
                    an_duration = payload.get("analysis_duration_sec")
                    if an_duration is not None:
                        self._log_an(
                            f"Analise finalizada. Run ID: {payload.get('run_id')} | "
                            f"Duracao: {format_duration_sec(float(an_duration))}"
                        )
                    else:
                        self._log_an(f"Analise finalizada. Run ID: {payload.get('run_id')}")
                    self.analysis_progress_var.set("progresso analise: finalizada")
                    self.var_send_run.set(payload.get("run_id", ""))
                    self.var_val_run.set(payload.get("run_id", ""))
                    self.var_run_id.set(payload.get("run_id", ""))
                    self._apply_batch_limit_for_run(payload.get("run_id", ""), notify=True, auto_set=True)
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
                        f"- chunks estimados: {payload.get('chunks_total')} ({payload.get('chunk_unit')})\n"
                        f"- batch max cmd (dcm4che): {payload.get('batch_max_cmd') or 'N/A'}\n"
                        f"- duracao analise: {format_duration_sec(float(payload.get('analysis_duration_sec') or 0))}"
                    )
                elif event == "send_progress":
                    done, total, cno, ctot, tech_no, _tech_total = payload
                    self.progress_items_var.set(f"enviando item {done} de {total}")
                    self.progress_chunks_var.set(f"batch chunk {cno} de {ctot} | retomada #{tech_no}")
                elif event == "send_done":
                    status = payload.get("status")
                    send_duration = payload.get("send_duration_sec")
                    if status == "ALREADY_SENT_PASS":
                        self._log_send(f"RUN ja enviado com sucesso anteriormente. Run ID: {payload.get('run_id')}")
                    else:
                        if send_duration is not None:
                            self._log_send(
                                f"SEND finalizado. Run ID: {payload.get('run_id')} | Status: {status} | "
                                f"Duracao: {format_duration_sec(float(send_duration))}"
                            )
                        else:
                            self._log_send(f"SEND finalizado. Run ID: {payload.get('run_id')} | Status: {status}")
                    self._refresh_run_list()
                elif event == "val_done":
                    val_duration = payload.get("validation_duration_sec")
                    if val_duration is not None:
                        self._log_val(
                            f"[VAL_END] Run ID: {payload.get('run_id')} | Status: {payload.get('status')} | "
                            f"Duracao: {format_duration_sec(float(val_duration))}"
                        )
                    else:
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
