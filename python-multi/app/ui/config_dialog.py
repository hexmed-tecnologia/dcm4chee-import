import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from app.config.settings import AppConfig
from app.shared.utils import normalize_dcm4che_iuid_update_mode, normalize_dcm4che_send_mode


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
        self.var_dcm4che_iuid_update_mode = tk.StringVar(
            value=normalize_dcm4che_iuid_update_mode(config.dcm4che_iuid_update_mode)
        )

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
        self.lbl_dcm4che_iuid_mode = ttk.Label(frm, text="Atualizacao de IUID (dcm4che)")
        self.lbl_dcm4che_iuid_mode.grid(row=8, column=0, sticky="w", pady=3)
        self.cmb_dcm4che_iuid_mode = ttk.Combobox(
            frm,
            textvariable=self.var_dcm4che_iuid_update_mode,
            values=["REALTIME", "CHUNK_END"],
            width=56,
            state="readonly",
        )
        self.cmb_dcm4che_iuid_mode.grid(row=8, column=1, sticky="we", pady=3)

        self.filter_frame = ttk.LabelFrame(frm, text="Filtro de arquivos para analise", padding=8)
        self.filter_frame.grid(row=9, column=0, columnspan=2, sticky="we", pady=(6, 0))
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
        ).grid(row=10, column=0, columnspan=2, sticky="w")
        self._row_entry(frm, 11, "TS mode", self.var_ts, combo_values=["AUTO", "JPEG_LS_LOSSLESS", "UNCOMPRESSED_STANDARD"])
        self._toggle_dcm4che_controls()

        btns = ttk.Frame(frm)
        btns.grid(row=12, column=0, columnspan=2, pady=(12, 0), sticky="e")
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
            self.lbl_dcm4che_iuid_mode.grid()
            self.cmb_dcm4che_iuid_mode.grid()
        else:
            self.lbl_dcm4che_mode.grid_remove()
            self.cmb_dcm4che_mode.grid_remove()
            self.lbl_dcm4che_iuid_mode.grid_remove()
            self.cmb_dcm4che_iuid_mode.grid_remove()
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
            dcm4che_iuid_update_mode=normalize_dcm4che_iuid_update_mode(self.var_dcm4che_iuid_update_mode.get().strip()),
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
