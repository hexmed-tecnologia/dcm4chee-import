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

## Politica de timestamp

- `run_id`: formato BR `ddMMyyyy_HHmmss`
- CSVs novos: colunas padrao `timestamp_br` e `timestamp_iso`
- Campos legados de data/hora (`generated_at`, `processed_at`, etc.) ficam em BR para leitura humana

## Toolkit Minimal

Esta aplicacao usa apenas um subconjunto das toolkits. O objetivo e reduzir bloat sem quebrar o runtime.

### dcm4che (`storescu` + `dcmdump`)

Perfil minimo validado:

- `bin/storescu.bat`
- `bin/dcmdump.bat`
- `etc/storescu/`
- `etc/dcmdump/`
- `etc/certs/`
- jars usados pelos launchers (`dcm4che-tool-storescu`, `dcm4che-tool-dcmdump`, `dcm4che-core`, `dcm4che-net`, `dcm4che-image`, `dcm4che-imageio`, `dcm4che-imageio-opencv`, `dcm4che-imageio-rle`, `dcm4che-tool-common`, `dcm4che-dict-priv`, `weasis-core-img`, `jai_imageio`, `clibwrapper_jiio`, `slf4j-api`, `logback-core`, `logback-classic`, `commons-cli`)
- `lib/windows-x86-64/opencv_java.dll`

Reducao estimada observada: de ~`172 MB` para ~`22 MB`.

### DCMTK - perfis

#### Perfil `runtime-now` (uso atual)

Arquivos minimos:

- `storescu.exe`
- `echoscu.exe`
- `dcmdump.exe`
- `dcmdata.dll`
- `dcmnet.dll`
- `dcmtls.dll`
- `oflog.dll`
- `ofstd.dll`

Tamanho aproximado: ~`6.1 MB`.

#### Perfil `with-jpls` (futuro com JPEG-LS)

Acrescentar ao `runtime-now`:

- `dcmcjpls.exe`
- `dcmdjpls.exe`
- `dcmimage.dll`
- `dcmimgle.dll`
- `dcmjpls.dll`
- `dcmtkcharls.dll`

Tamanho aproximado: ~`9.3 MB`.

#### Perfil `jpeg-family` (opcional mais amplo)

Acrescentar ao `with-jpls`:

- `dcmcjpeg.exe`
- `dcmdjpeg.exe`
- `dcmcrle.exe`
- `dcmdrle.exe`
- `dcmjpeg.dll`
- `ijg8.dll`
- `ijg12.dll`
- `ijg16.dll`

Tamanho aproximado: ~`10.1 MB`.

### Checklist pos-limpeza

Executar no host de build/runtime:

```powershell
.\toolkits\dcm4che-5.34.2\bin\storescu.bat --help
.\toolkits\dcm4che-5.34.2\bin\dcmdump.bat --help
.\toolkits\dcmtk-3.6.7-win64-dynamic\bin\storescu.exe --version
.\toolkits\dcmtk-3.6.7-win64-dynamic\bin\echoscu.exe --version
.\toolkits\dcmtk-3.6.7-win64-dynamic\bin\dcmdump.exe --version
```

## Build onedir

Use:

```powershell
.\build-scripts\build_onedir.bat
```

Saida em:

- `build-scripts\artifacts\dist\<timestamp>\DicomMultiToolkit\DicomMultiToolkit.exe`

