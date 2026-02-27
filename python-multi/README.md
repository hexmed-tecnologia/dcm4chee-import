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
   - `run_id` automatico passa a incluir sufixo do toolkit/modo (ex.: `26022026_123000_dcm4che_folders`, `26022026_123500_dcm4che_files`, `26022026_124000_dcmtk`)
2. Revisar dashboard de analise.
3. Rodar `Send` por `run_id`.
4. Rodar `Validacao` manualmente por `run_id`.
5. Opcional: na aba `Validacao`, usar `Exportar relatorio completo` com modo:
   - `A - por arquivo`
   - `C - por StudyUID`

## Artefatos por run

Cada `run_id` agora organiza os artefatos em subpastas:

- `core/`
  - `manifest_folders.csv`
  - `manifest_files.csv`
  - `analysis_summary.csv`
  - `send_results_by_file.csv`
  - `send_summary.csv`
  - `validation_results.csv`
  - `send_checkpoint_dcm4che_folders.json` ou `send_checkpoint_dcm4che_files.json` (quando toolkit = dcm4che)
  - `send_checkpoint_dcmtk.json` (quando toolkit = dcmtk)
  - `batch_args/`
- `telemetry/`
  - `events.csv` (telemetria consolidada do run)
  - `storescu_execucao.log`
- `reports/`
  - `reconciliation_report.csv`
  - `validation_full_report_A.csv` (quando exportado no modo A)
  - `validation_full_report_C.csv` (quando exportado no modo C)

Compatibilidade: runs antigos sem subpastas continuam sendo lidos automaticamente (fallback para layout legado).
Compatibilidade de schema: arquivos legados de `core` (`file_iuid_map.csv`, `validation_by_iuid.csv`, `validation_by_file.csv`) sao lidos como fallback quando presentes, mas novos runs passam a consolidar esses dados em `send_results_by_file.csv` e `validation_results.csv`.

## Telemetria consolidada por run

Arquivo principal:

- `telemetry/events.csv`

Cabeçalho:

- `run_id`
- `event_type`
- `timestamp_iso`
- `message`
- `ref`

Cada linha registra um evento do fluxo (`Analise`, `Send`, validação de consistência). Os CSVs abaixo não são mais gerados para telemetria a partir de agora:

- `analysis_events.csv`
- `send_events.csv`
- `send_errors.csv`
- `consistency_events.csv`

Exemplos práticos de leitura:

- Início e fim da análise: `ANALYSIS_END`, `ANALYSIS_CANCELLED`
- Envio:
  - `RUN_SEND_START`, `CHUNK_START`, `CHUNK_END`, `RUN_SEND_END`
  - Falha por item: `SEND_FILE_ERROR` (use `ref` para localizar `file_path` e `error_type`)
  - Erros de parsing/scan no storescu: `SEND_PARSE_EXCEPTION`
  - Marcadores de diagnostico no log: `[SEND_PARSE_MISMATCH]` e `[RUN_ID_GUARD]`
- Consistência/validação: `CONSISTENCY_FILLED`, `CONSISTENCY_MISSING`
- Execução sem trabalho novo: `RUN_SEND_SKIP_ALREADY_COMPLETED`

Exemplo de fluxo rápido no PowerShell:

```powershell
$run = "<run_id>"
Import-Csv "python-multi\runs\$run\telemetry\events.csv" -Delimiter ";" |
  Sort-Object timestamp_iso |
  Format-Table timestamp_iso,event_type,ref,message -AutoSize
```

```powershell
Import-Csv "python-multi\runs\$run\telemetry\events.csv" -Delimiter ";" |
  Where-Object event_type -in @("SEND_FILE_ERROR","RUN_SEND_END","CONSISTENCY_MISSING") |
  Sort-Object timestamp_iso
```

## Relatorio completo (Validacao)

Campos principais exportados:

- `nome_paciente`
- `data_nascimento`
- `prontuario`
- `accession_number`
- `sexo`
- `data_exame`
- `descricao_exame`
- `study_uid`
- `status` (`OK` quando IUID encontrado no dcm4chee, `ERRO` quando nao encontrado ou com falha de consulta)

## Configuracao

No menu `Configuracao -> Configuracoes`:

- Toolkit ativo (`dcm4che` ou `dcmtk`)
- Modo de envio dcm4che (`MANIFEST_FILES` padrao, ou `FOLDERS`) quando toolkit = `dcm4che`
- AET origem (padrao: `HMD_IMPORTER`), AET destino
- PACS DICOM host (C-STORE) e PACS DICOM port (C-STORE)
- Host REST para validacao
- Tamanho de batch
- Regras de indexacao por extensao
  - lista separada por virgula, ex.: `.dcm,.ima,.dicom`
  - opcao `Nao restringir por extensao (incluir todos os arquivos)` (desmarcada por padrao)
  - opcao `Incluir arquivos sem extensao` (aplicada quando a restricao por extensao estiver ativa)
  - em `dcm4che + FOLDERS`, esse bloco fica inativo e a analise considera todos os arquivos para manter coerencia com o envio por pasta
- Opcao de calcular `size_bytes` na analise (desmarcada por padrao para melhor performance)
- Modo TS (`AUTO`, `JPEG_LS_LOSSLESS`, `UNCOMPRESSED_STANDARD`)

Observacao: o campo `Runs base dir` nao e mais exibido na interface. O app usa o caminho local padrao `python-multi/runs`.
Ao trocar toolkit/modo de envio, o app limpa automaticamente o campo `Run ID (opcional)` para evitar sufixo inconsistente; nao e necessario reiniciar.

Observacao: nesta versao, apenas `AUTO` esta ativo. Os demais modos estao estruturados para evolucao futura.
As toolkits sao sempre resolvidas internamente em `toolkits/<nome>-*/bin` relativo ao app (sem configuracao manual de path).

Durante a analise, o log pode exibir marcadores `[AN_SCAN_PROGRESS]` com taxa de varredura e ETA aproximado.
O dashboard da aba `Analise` tambem exibe um indicador visual de progresso com ETA aproximado em tempo real.
No menu principal ha um item `Sobre` que mostra a versao atual da aplicacao.

## Versionamento

- Arquivo fonte de versao: `VERSION` (na raiz de `python-multi`)
- Versao inicial: `v1.0.0`
- A janela principal exibe a versao na barra de titulo (`DICOM Multi Toolkit - vX.Y.Z`)
- O menu `Sobre` exibe a mesma versao
- Todo build via `build-scripts\build_onedir.bat` le a versao desse arquivo automaticamente

## Politica de timestamp

- `run_id`: formato BR `ddMMyyyy_HHmmss`
- `events.csv` usa `timestamp_iso` como coluna padrão.
- Demais CSVs novos continuam com o padrão da aplicação (`timestamp_br` e `timestamp_iso`) quando aplicável.
- Campos legados de data/hora (`generated_at`, `processed_at`, etc.) ficam em BR para leitura humana.

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

- `build-scripts\dist\<timestamp_br>\DicomMultiToolkit\DicomMultiToolkit.exe`

Formato de `<timestamp_br>`: `ddMMyyyy_HHmmss`

