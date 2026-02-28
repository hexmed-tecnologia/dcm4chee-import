import json
import threading
import time
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest

from app.config.settings import AppConfig
from app.infra.run_artifacts import (
    apply_send_result_updates,
    build_iuid_map_from_send_rows,
    cleanup_run_artifact_variants,
    merge_iuid_map_from_legacy_file,
    read_csv_rows,
    resolve_run_artifact_path,
    write_csv_row,
    write_csv_table,
    write_telemetry_event,
)
from app.integrations.toolkit_drivers import apply_internal_toolkit_paths, get_driver
from app.shared.utils import format_duration_sec, now_br


class ValidationWorkflow:
    def __init__(self, cfg: AppConfig, logger, cancel_event: threading.Event):
        self.cfg = cfg
        self.logger = logger
        self.cancel_event = cancel_event
        apply_internal_toolkit_paths(self.cfg, Path(__file__).resolve().parent.parent.parent, self._log)
        self.driver = get_driver(cfg.toolkit)

    def _log(self, msg: str) -> None:
        self.logger(msg)

    def _resolve_runs_base(self, script_dir: Path) -> Path:
        if self.cfg.runs_base_dir.strip():
            p = Path(self.cfg.runs_base_dir.strip())
            if p.is_absolute():
                return p
            return (script_dir / p).resolve()
        return (script_dir / "runs").resolve()

    def _query_instance_dataset(self, iuid: str) -> dict:
        url = f"http://{self.cfg.pacs_rest_host}/dcm4chee-arc/aets/{self.cfg.aet_destino}/rs/instances?SOPInstanceUID={iuid}"
        api_found = 0
        http_status = ""
        detail = ""
        dataset: dict = {}
        try:
            req = urlrequest.Request(url, method="GET")
            with urlrequest.urlopen(req, timeout=20) as resp:
                http_status = str(resp.status)
                body = resp.read().decode("utf-8", errors="replace")
                data = json.loads(body) if body.strip() else []
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    api_found = 1
                    dataset = data[0]
        except urlerror.HTTPError as ex:
            http_status = str(ex.code)
            detail = str(ex)
        except Exception as ex:
            http_status = "ERR"
            detail = str(ex)
        return {
            "api_found": api_found,
            "http_status": http_status,
            "detail": detail,
            "dataset": dataset,
        }

    def _dicom_text(self, dataset: dict, tag: str) -> str:
        elem = dataset.get(tag, {})
        if not isinstance(elem, dict):
            return ""
        values = elem.get("Value", [])
        if not isinstance(values, list) or not values:
            return ""
        first = values[0]
        if isinstance(first, dict):
            if "Alphabetic" in first:
                return str(first.get("Alphabetic", "")).strip()
            for v in first.values():
                if v is not None:
                    return str(v).strip()
            return ""
        return str(first).strip()

    def _report_fields_from_dataset(self, dataset: dict) -> dict:
        return {
            "nome_paciente": self._dicom_text(dataset, "00100010"),
            "data_nascimento": self._dicom_text(dataset, "00100030"),
            "prontuario": self._dicom_text(dataset, "00100020"),
            "accession_number": self._dicom_text(dataset, "00080050"),
            "sexo": self._dicom_text(dataset, "00100040"),
            "data_exame": self._dicom_text(dataset, "00080020"),
            "descricao_exame": self._dicom_text(dataset, "00081030"),
            "study_uid": self._dicom_text(dataset, "0020000D"),
        }

    def export_complete_report(self, run_id: str, report_mode: str = "A") -> dict:
        run = run_id.strip()
        if not run:
            raise RuntimeError("run_id e obrigatorio para exportar relatorio.")
        mode = (report_mode or "A").strip().upper()
        if mode not in ["A", "C"]:
            raise RuntimeError(f"Modo de relatorio invalido: {report_mode}")

        script_dir = Path(__file__).resolve().parent.parent.parent
        run_dir = self._resolve_runs_base(script_dir) / run
        if not run_dir.exists():
            raise RuntimeError(f"Run nao encontrado: {run_dir}")
        self._log("[RUN_LAYOUT] mode=report_export layout=core|telemetry|reports")

        send_results = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=True, logger=self._log)
        legacy_file_iuid_map = resolve_run_artifact_path(run_dir, "file_iuid_map.csv", for_write=False, logger=self._log)
        if not send_results.exists():
            raise RuntimeError(f"Arquivo nao encontrado: {send_results}")

        send_rows = read_csv_rows(send_results)
        map_by_file = build_iuid_map_from_send_rows(send_rows)
        merge_iuid_map_from_legacy_file(map_by_file, legacy_file_iuid_map)

        sent_ok_rows = [r for r in send_rows if r.get("send_status", "") == "SENT_OK"]
        if not sent_ok_rows:
            raise RuntimeError("Nenhum arquivo SENT_OK encontrado para exportacao.")

        report_records: list[dict] = []
        updates_by_file: dict[str, dict] = {}
        for row in sent_ok_rows:
            fp = row.get("file_path", "").strip()
            if not fp:
                continue
            meta = map_by_file.get(fp, {})
            iuid = str(meta.get("sop_instance_uid", "")).strip()
            if not iuid:
                iuid, ts_uid, ts_name, err = self.driver.extract_metadata(self.cfg, Path(fp))
                if iuid:
                    map_by_file[fp] = {
                        "sop_instance_uid": iuid,
                        "source_ts_uid": ts_uid,
                        "source_ts_name": ts_name,
                        "extract_status": "REPORT_EXPORT_OK",
                    }
                    updates_by_file[fp] = {
                        "sop_instance_uid": iuid,
                        "source_ts_uid": ts_uid,
                        "source_ts_name": ts_name,
                        "extract_status": "REPORT_EXPORT_OK",
                    }
                else:
                    self._log(f"[WARN] IUID ausente para arquivo no relatorio: {fp} | erro={err or 'desconhecido'}")
            report_records.append({"file_path": fp, "sop_instance_uid": iuid})

        updated_rows = apply_send_result_updates(send_results, run, updates_by_file)
        if updated_rows > 0:
            self._log(f"[CORE_COMPACT] send_results_by_file atualizado com IUID para {updated_rows} arquivo(s).")

        unique_iuids = sorted({r["sop_instance_uid"] for r in report_records if r["sop_instance_uid"]})
        self._log(f"[REPORT_EXPORT] Modo {mode} | IUIDs unicos para consulta: {len(unique_iuids)}")

        iuid_data: dict[str, dict] = {}
        done = 0
        for iuid in unique_iuids:
            if self.cancel_event.is_set():
                raise RuntimeError("Exportacao de relatorio cancelada.")
            query = self._query_instance_dataset(iuid)
            fields = self._report_fields_from_dataset(query.get("dataset", {}))
            status = "OK" if query.get("api_found", 0) == 1 else "ERRO"
            iuid_data[iuid] = {
                **fields,
                "status": status,
                "http_status": str(query.get("http_status", "")),
                "detail": str(query.get("detail", "")),
            }
            done += 1
            if done % 100 == 0:
                self._log(f"[REPORT_EXPORT_PROGRESS] {done}/{len(unique_iuids)} IUIDs consultados")

        rows_a: list[dict] = []
        for rec in report_records:
            fp = rec.get("file_path", "")
            iuid = rec.get("sop_instance_uid", "")
            base = iuid_data.get(
                iuid,
                {
                    "nome_paciente": "",
                    "data_nascimento": "",
                    "prontuario": "",
                    "accession_number": "",
                    "sexo": "",
                    "data_exame": "",
                    "descricao_exame": "",
                    "study_uid": "",
                    "status": "ERRO",
                    "http_status": "",
                    "detail": "IUID ausente",
                },
            )
            rows_a.append(
                {
                    "run_id": run,
                    "file_path": fp,
                    "sop_instance_uid": iuid,
                    "nome_paciente": base.get("nome_paciente", ""),
                    "data_nascimento": base.get("data_nascimento", ""),
                    "prontuario": base.get("prontuario", ""),
                    "accession_number": base.get("accession_number", ""),
                    "sexo": base.get("sexo", ""),
                    "data_exame": base.get("data_exame", ""),
                    "descricao_exame": base.get("descricao_exame", ""),
                    "study_uid": base.get("study_uid", ""),
                    "status": base.get("status", "ERRO"),
                }
            )

        if mode == "A":
            report_file = resolve_run_artifact_path(
                run_dir, "validation_full_report_A.csv", for_write=True, logger=self._log, keep_legacy_on_write=False
            )
            fieldnames = [
                "run_id",
                "file_path",
                "sop_instance_uid",
                "nome_paciente",
                "data_nascimento",
                "prontuario",
                "accession_number",
                "sexo",
                "data_exame",
                "descricao_exame",
                "study_uid",
                "status",
            ]
            write_csv_table(report_file, rows_a, fieldnames)
            status_ok = sum(1 for r in rows_a if r.get("status") == "OK")
            status_err = len(rows_a) - status_ok
            self._log(f"[REPORT_EXPORT] Relatorio A exportado: {report_file} | linhas={len(rows_a)} ok={status_ok} erro={status_err}")
            return {"run_id": run, "mode": mode, "report_file": str(report_file), "rows": len(rows_a), "ok": status_ok, "erro": status_err}

        grouped: dict[str, dict] = {}
        for row in rows_a:
            study_uid = row.get("study_uid", "").strip()
            key = study_uid if study_uid else f"__ERRO__{row.get('sop_instance_uid', '').strip() or row.get('file_path', '').strip()}"
            agg = grouped.setdefault(
                key,
                {
                    "run_id": run,
                    "study_uid": study_uid,
                    "nome_paciente": "",
                    "data_nascimento": "",
                    "prontuario": "",
                    "accession_number": "",
                    "sexo": "",
                    "data_exame": "",
                    "descricao_exame": "",
                    "status": "OK",
                    "total_arquivos": 0,
                },
            )
            agg["total_arquivos"] = int(agg.get("total_arquivos", 0)) + 1
            for f in ["nome_paciente", "data_nascimento", "prontuario", "accession_number", "sexo", "data_exame", "descricao_exame"]:
                if not agg.get(f):
                    agg[f] = row.get(f, "")
            if not agg.get("study_uid"):
                agg["study_uid"] = study_uid
            if row.get("status", "ERRO") == "ERRO":
                agg["status"] = "ERRO"

        rows_c = sorted(grouped.values(), key=lambda x: str(x.get("study_uid", "")))
        report_file = resolve_run_artifact_path(
            run_dir, "validation_full_report_C.csv", for_write=True, logger=self._log, keep_legacy_on_write=False
        )
        fieldnames = [
            "run_id",
            "study_uid",
            "nome_paciente",
            "data_nascimento",
            "prontuario",
            "accession_number",
            "sexo",
            "data_exame",
            "descricao_exame",
            "status",
            "total_arquivos",
        ]
        write_csv_table(report_file, rows_c, fieldnames)
        status_ok = sum(1 for r in rows_c if r.get("status") == "OK")
        status_err = len(rows_c) - status_ok
        self._log(f"[REPORT_EXPORT] Relatorio C exportado: {report_file} | linhas={len(rows_c)} ok={status_ok} erro={status_err}")
        return {"run_id": run, "mode": mode, "report_file": str(report_file), "rows": len(rows_c), "ok": status_ok, "erro": status_err}

    def run_validation(self, run_id: str) -> dict:
        validation_start_ts = time.monotonic()
        run = run_id.strip()
        if not run:
            raise RuntimeError("run_id e obrigatorio para validacao.")
        script_dir = Path(__file__).resolve().parent.parent.parent
        run_dir = self._resolve_runs_base(script_dir) / run
        if not run_dir.exists():
            raise RuntimeError(f"Run nao encontrado: {run_dir}")
        self._log("[RUN_LAYOUT] mode=validation layout=core|telemetry|reports")

        send_results = resolve_run_artifact_path(run_dir, "send_results_by_file.csv", for_write=True, logger=self._log)
        legacy_file_iuid_map = resolve_run_artifact_path(run_dir, "file_iuid_map.csv", for_write=False, logger=self._log)
        for filename in ["validation_results.csv", "validation_by_iuid.csv", "validation_by_file.csv", "reconciliation_report.csv"]:
            cleanup_run_artifact_variants(run_dir, filename)
        events = resolve_run_artifact_path(run_dir, "events.csv", for_write=True, logger=self._log)
        validation_results = resolve_run_artifact_path(run_dir, "validation_results.csv", for_write=True, logger=self._log)
        recon = resolve_run_artifact_path(run_dir, "reconciliation_report.csv", for_write=True, logger=self._log)

        send_rows = read_csv_rows(send_results)
        map_by_file = build_iuid_map_from_send_rows(send_rows)
        merge_iuid_map_from_legacy_file(map_by_file, legacy_file_iuid_map)

        total_send_rows = len(send_rows)
        send_ok_files = sum(1 for r in send_rows if r.get("send_status", "") == "SENT_OK")
        send_warn_files = sum(1 for r in send_rows if r.get("send_status", "") in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"])
        send_fail_files = sum(1 for r in send_rows if r.get("send_status", "") == "SEND_FAIL")
        self._log(f"[VAL_START] run_id={run}")
        self._log(
            f"[VAL_RESULT] send_total={total_send_rows} sent_ok={send_ok_files} "
            f"warn={send_warn_files} fail={send_fail_files}"
        )
        self._log(f"Mapeamentos IUID atuais (send_results+fallback legado): {len(map_by_file)}")
        write_telemetry_event(
            events,
            run,
            "VALIDATION_START",
            "Validacao iniciada.",
            (
                f"send_rows={total_send_rows};sent_ok={send_ok_files};send_warn={send_warn_files};"
                f"send_fail={send_fail_files};mapped_iuid={len(map_by_file)}"
            ),
        )

        updates_by_file: dict[str, dict] = {}
        # consistency check: complete missing IUIDs before API calls
        for row in send_rows:
            if row.get("send_status", "") != "SENT_OK":
                continue
            fp = row.get("file_path", "").strip()
            if not fp or fp in map_by_file:
                continue
            iuid, ts_uid, ts_name, err = self.driver.extract_metadata(self.cfg, Path(fp))
            if iuid:
                map_by_file[fp] = {
                    "sop_instance_uid": iuid,
                    "source_ts_uid": ts_uid,
                    "source_ts_name": ts_name,
                    "extract_status": "CONSISTENCY_OK",
                }
                updates_by_file[fp] = {
                    "sop_instance_uid": iuid,
                    "source_ts_uid": ts_uid,
                    "source_ts_name": ts_name,
                    "extract_status": "CONSISTENCY_OK",
                }
                write_telemetry_event(
                    events,
                    run,
                    "CONSISTENCY_FILLED",
                    "IUID preenchido antes da validacao.",
                    f"file_path={fp}",
                )
            else:
                write_telemetry_event(
                    events,
                    run,
                    "CONSISTENCY_MISSING",
                    err or "Nao foi possivel extrair IUID.",
                    f"file_path={fp}",
                )

        updated_rows = apply_send_result_updates(send_results, run, updates_by_file)
        if updated_rows > 0:
            self._log(f"[CORE_COMPACT] send_results_by_file atualizado pela consistencia em {updated_rows} arquivo(s).")

        iuid_to_files: dict[str, list[str]] = {}
        for row in send_rows:
            if row.get("send_status", "") != "SENT_OK":
                continue
            fp = row.get("file_path", "").strip()
            iuid = str(map_by_file.get(fp, {}).get("sop_instance_uid", "")).strip()
            if not iuid:
                continue
            iuid_to_files.setdefault(iuid, []).append(fp)

        self._log(f"IUIDs unicos para consulta API: {len(iuid_to_files)}")

        validation_fields = [
            "run_id",
            "file_path",
            "sop_instance_uid",
            "send_status",
            "validation_status",
            "api_found",
            "http_status",
            "detail",
            "checked_at",
        ]

        ok_count = 0
        miss_count = 0
        api_err_count = 0
        for iuid, files in iuid_to_files.items():
            if self.cancel_event.is_set():
                raise RuntimeError("Validacao cancelada.")
            url = f"http://{self.cfg.pacs_rest_host}/dcm4chee-arc/aets/{self.cfg.aet_destino}/rs/instances?SOPInstanceUID={iuid}"
            api_found = 0
            http_status = ""
            detail = ""
            try:
                req = urlrequest.Request(url, method="GET")
                with urlrequest.urlopen(req, timeout=20) as resp:
                    http_status = str(resp.status)
                    body = resp.read().decode("utf-8", errors="replace")
                    data = json.loads(body) if body.strip() else []
                    api_found = 1 if isinstance(data, list) and len(data) > 0 else 0
            except urlerror.HTTPError as ex:
                http_status = str(ex.code)
                detail = str(ex)
            except Exception as ex:
                http_status = "ERR"
                detail = str(ex)

            if api_found == 1:
                ok_count += 1
            else:
                if http_status in ["ERR", ""]:
                    api_err_count += 1
                else:
                    miss_count += 1

            status = "OK" if api_found == 1 else ("API_ERROR" if http_status in ["ERR", ""] else "NOT_FOUND")
            for fp in files:
                write_csv_row(
                    validation_results,
                    {
                        "run_id": run,
                        "file_path": fp,
                        "sop_instance_uid": iuid,
                        "send_status": "SENT_OK",
                        "validation_status": status,
                        "api_found": api_found,
                        "http_status": http_status,
                        "detail": detail,
                        "checked_at": now_br(),
                    },
                    validation_fields,
                )
            if (ok_count + miss_count + api_err_count) % 100 == 0:
                self._log(
                    f"Progresso validacao API: {ok_count + miss_count + api_err_count}/{len(iuid_to_files)} "
                    f"(ok={ok_count}, nf={miss_count}, api_err={api_err_count})"
                )

        warnings_count = 0
        fail_count = 0
        for row in send_rows:
            st = row.get("send_status", "")
            if st in ["NON_DICOM", "UNSUPPORTED_DICOM_OBJECT", "SENT_UNKNOWN"]:
                warnings_count += 1
            elif st in ["SEND_FAIL"]:
                fail_count += 1

        final_status = "PASS"
        if fail_count > 0 or api_err_count > 0 or miss_count > 0:
            final_status = "PASS_WITH_WARNINGS"
        if api_err_count > 0 and ok_count == 0:
            final_status = "FAIL"
        validation_duration_sec = round(max(time.monotonic() - validation_start_ts, 0.0), 3)

        write_csv_row(
            recon,
            {
                "run_id": run,
                "toolkit": self.cfg.toolkit,
                "total_iuid_unique": len(iuid_to_files),
                "iuid_ok": ok_count,
                "iuid_not_found": miss_count,
                "iuid_api_error": api_err_count,
                "send_warning_files": warnings_count,
                "send_failed_files": fail_count,
                "final_status": final_status,
                "validation_duration_sec": validation_duration_sec,
                "generated_at": now_br(),
            },
            [
                "run_id",
                "toolkit",
                "total_iuid_unique",
                "iuid_ok",
                "iuid_not_found",
                "iuid_api_error",
                "send_warning_files",
                "send_failed_files",
                "final_status",
                "validation_duration_sec",
                "generated_at",
            ],
        )
        self._log("[VAL_RESULT] --- Resumo Final Validacao ---")
        self._log(f"Run ID: {run}")
        self._log(f"Arquivos do send: {total_send_rows}")
        self._log(f"Arquivos SENT_OK: {send_ok_files}")
        self._log(f"Arquivos com warning no send: {send_warn_files}")
        self._log(f"Arquivos com falha no send: {send_fail_files}")
        self._log(f"IUIDs unicos consultados: {len(iuid_to_files)}")
        self._log(f"IUIDs OK: {ok_count}")
        self._log(f"IUIDs NOT_FOUND: {miss_count}")
        self._log(f"IUIDs API_ERROR: {api_err_count}")
        self._log(f"[VAL_END] run_id={run} status={final_status} duration={format_duration_sec(validation_duration_sec)}")
        write_telemetry_event(
            events,
            run,
            "VALIDATION_END",
            "Validacao finalizada.",
            (
                f"status={final_status};iuid_total={len(iuid_to_files)};iuid_ok={ok_count};"
                f"iuid_not_found={miss_count};iuid_api_error={api_err_count};"
                f"validation_duration_sec={validation_duration_sec}"
            ),
        )
        return {"run_id": run, "status": final_status, "run_dir": str(run_dir), "validation_duration_sec": validation_duration_sec}
