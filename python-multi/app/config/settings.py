from dataclasses import dataclass


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
    dcm4che_iuid_update_mode: str = "REALTIME"
    # Prefer direct Java launcher with @argfile on Windows to avoid cmd line-length bottlenecks.
    dcm4che_prefer_java_direct: bool = True
    # Internal flag: keep Windows-stable wrapper for .bat execution by default.
    dcm4che_use_shell_wrapper: bool = True
