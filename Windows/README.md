# Fluxo de envio e validacao por run

Este diretório contém scripts para envio DICOM e validação posterior no dcm4chee, com rastreabilidade por `run_id`.

## Scripts

- `send.ps1`: envia por lotes de pastas-fim, gera artefatos de envio por execução.
- `valida_run.ps1`: valida, em execução separada, os IUIDs de sucesso de um `run_id`.
- `valida.ps1` e `valida2.ps1`: scripts legados (mantidos sem alteração).

## Como funciona o `send.ps1`

1. Descobre recursivamente todas as pastas-fim da raiz informada.
2. Cria `manifest_folders.csv` para congelar o universo da execução.
3. Processa as pastas em batch (`$tamanhoBatchPastas`, padrão 50).
4. Para cada pasta:
   - envia via `storescu.bat`;
   - grava log completo;
   - extrai IUIDs de sucesso/erro por regex;
   - atualiza checkpoint.
5. Permite retomada de run interrompido com `runIdParaRetomar`.

> Regra de `SKIPPED_EMPTY`: pasta-fim sem nenhum arquivo.  
> Não faz detecção de tipo DICOM para decidir esse skip.

## Estrutura de runs

Por padrão, os runs ficam em:

- `<diretorio_do_script>\runs\<run_id>\`

Se `runsBaseDir` for preenchido no script, usa esse caminho.

Arquivos gerados por run:

- `manifest_folders.csv`
- `storescu_execucao.log`
- `sucesso_iuids.txt`
- `erro_iuids.txt`
- `send_checkpoint.json`
- `folder_results.csv`
- `send_events.csv`
- `send_summary.csv`
- `validation_report.csv` (quando `valida_run.ps1` for executado)
- `validation_events.csv` (quando `valida_run.ps1` for executado)
- `reconciliation_report.csv` (quando `valida_run.ps1` for executado)
- `nao_validados_iuids.txt` (quando existir pendência na validação)

## Como funciona o `valida_run.ps1`

1. Recebe `runId` (obrigatório).
2. Lê `sucesso_iuids.txt` e valida cada IUID no dcm4chee (sem retry).
3. Gera:
   - `validation_report.csv` (resultado por IUID);
   - `reconciliation_report.csv` com status final:
     - `PASS`
     - `PASS_WITH_WARNINGS`
     - `FAIL`

## Uso recomendado

1. Executar `send.ps1`.
2. Aguardar processamento no dcm4chee.
3. Executar `valida_run.ps1` com o `run_id` desejado.
4. Se houver pendências, usar os relatórios para reprocessar apenas necessários.

## Cancelamento

Durante envio:

- padrão: `Ctrl+C`
- emergência: `taskkill /F /T /IM java.exe`
