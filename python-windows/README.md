# MVP Python Windows (Tkinter)

Aplicacao desktop (MVP) para envio e validacao DICOM com abas:

- `Send`
- `Validacao`
- `Runs`

Usa os binarios do `dcm4che` embutidos no projeto e gera artefatos por `run_id`.

## Requisitos

- Windows
- Python 3.10+
- Java instalado e disponivel no sistema (`java -version`)
- Pasta `dcm4che-5.34.2` dentro de `python-windows`

## Como executar

No terminal, dentro de `python-windows`:

```powershell
python app.py
```

## Build onedir (.exe)

Para gerar build de teste para cliente (sem instalador), use:

```powershell
.\build-scripts\build_onedir.bat
```

Detalhes:

- O script fixa Python `3.12` via `py -3.12`.
- Cria/usa venv local em `build-scripts\artifacts\.venv-build`.
- Instala `pyinstaller` automaticamente.
- Limpa pastas temporarias (`build` e `spec`) antes de cada novo build.
- Inclui a pasta `dcm4che-5.34.2` dentro do `dist`.
- Cria uma pasta nova com timestamp em cada execucao.
- Saida final em `build-scripts\artifacts\dist\<timestamp>\DicomSenderValidator\DicomSenderValidator.exe`.

## Configuracao

No menu `Configuracao -> Configuracoes DCM4CHEE`:

- `dcm4che bin path` (deve apontar para pasta com `storescu.bat`)
- `AET destino`
- `PACS host`
- `PACS port`
- `PACS REST host:porta`
- `Runs base dir` (opcional)
- `Batch default`

### Teste de Echo

Na janela de configuracao, o botao `Testar Echo` executa `storescu.bat` sem arquivo de entrada (modo C-ECHO) para validar conectividade.

## Fluxo SEND

- Descobre pastas-fim da raiz de exames.
- Gera `manifest_folders.csv`.
- Envia em lotes (`batch`) via `storescu.bat`.
- Atualiza `sucesso_iuids.txt` e `erro_iuids.txt` incrementalmente.
- Salva checkpoint (`send_checkpoint.json`) para retomada.

## Fluxo VALIDACAO

- Seleciona `run_id`.
- Valida IUIDs de `sucesso_iuids.txt` no dcm4chee (sem retry).
- Gera `validation_report.csv` e `reconciliation_report.csv`.
- Status final: `PASS`, `PASS_WITH_WARNINGS` ou `FAIL`.

## Estrutura por run

Na pasta de runs (`runs/<run_id>/`), a app gera:

- `manifest_folders.csv`
- `storescu_execucao.log`
- `sucesso_iuids.txt`
- `erro_iuids.txt`
- `send_checkpoint.json`
- `folder_results.csv`
- `send_events.csv`
- `send_summary.csv`
- `validation_report.csv`
- `validation_events.csv`
- `reconciliation_report.csv`
- `nao_validados_iuids.txt` (se houver)

## Observacoes

- `SKIPPED_EMPTY` significa pasta-fim sem nenhum arquivo.
- A validacao nao e automatica apos o send; e executada manualmente na aba `Validacao`.
