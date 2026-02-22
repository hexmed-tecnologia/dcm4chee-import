# Python Multi Toolkit (MVP)

Aplicacao desktop em Tkinter para fluxo DICOM com dois toolkits:

- `dcm4che-tools`
- `DCMTK`

## Abas principais

- `Analise`
- `Send`
- `Validacao`
- `Runs`

## Fluxo

1. Rodar `Analise` para gerar manifestos:
   - `manifest_folders.csv`
   - `manifest_files.csv`
   - `analysis_summary.csv`
2. Revisar dashboard de analise.
3. Rodar `Send` por `run_id`.
4. Rodar `Validacao` manualmente por `run_id`.

## Artefatos por run

- `manifest_folders.csv`
- `manifest_files.csv`
- `analysis_summary.csv`
- `analysis_events.csv`
- `storescu_execucao.log`
- `send_events.csv`
- `send_results_by_file.csv`
- `send_errors.csv`
- `file_iuid_map.csv`
- `send_summary.csv`
- `consistency_events.csv`
- `validation_by_iuid.csv`
- `validation_by_file.csv`
- `reconciliation_report.csv`

## Configuracao

No menu `Configuracao -> Configuracoes`:

- Toolkit ativo (`dcm4che` ou `dcmtk`)
- Caminho do binario `dcm4che` (opcional, se usar toolkit externo)
- Caminho do binario `dcmtk` (opcional, se usar toolkit externo)
- AET origem, AET destino
- Host e porta do PACS
- Host REST para validacao
- Tamanho de batch
- Regras de indexacao por extensao
- Modo TS (`AUTO`, `JPEG_LS_LOSSLESS`, `UNCOMPRESSED_STANDARD`)

Observacao: nesta versao, apenas `AUTO` esta ativo. Os demais modos estao estruturados para evolucao futura.

## Build onedir

Use:

```powershell
.\build-scripts\build_onedir.bat
```

Saida em:

- `build-scripts\artifacts\dist\<timestamp>\DicomMultiToolkit\DicomMultiToolkit.exe`

