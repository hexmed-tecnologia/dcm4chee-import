# 1. Configuracoes
$runId = "" # Obrigatorio para escolher qual run validar.
$runsBaseDir = "" # Se vazio, usa "<diretorio_do_script>\runs"
$aet = "HMD_IMPORTED"
$ipServer = "192.168.1.70:8080"
$nivelLogMinimo = "INFO" # DEBUG, INFO, WARN, ERROR

$ordemNivelLog = @{
    DEBUG = 10
    INFO  = 20
    WARN  = 30
    ERROR = 40
}

function Get-ScriptDirectory {
    if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) {
        return $PSScriptRoot
    }
    if ($MyInvocation.MyCommand.Path) {
        return (Split-Path -Parent $MyInvocation.MyCommand.Path)
    }
    return (Get-Location).Path
}

function Write-Log {
    param(
        [string]$Nivel,
        [string]$Mensagem
    )

    if (-not $ordemNivelLog.ContainsKey($Nivel)) { return }
    if ($ordemNivelLog[$Nivel] -lt $ordemNivelLog[$nivelLogMinimo]) { return }

    $cor = "White"
    switch ($Nivel) {
        "DEBUG" { $cor = "DarkGray" }
        "INFO"  { $cor = "Cyan" }
        "WARN"  { $cor = "Yellow" }
        "ERROR" { $cor = "Red" }
    }
    Write-Host "[$Nivel] $Mensagem" -ForegroundColor $cor
}

function Write-ValidationEvent {
    param(
        [string]$EventFile,
        [string]$RunId,
        [string]$Level,
        [string]$EventType,
        [string]$Message,
        [string]$Iuid = "",
        [string]$Extra = ""
    )

    $row = [PSCustomObject]@{
        timestamp  = (Get-Date).ToString("s")
        run_id     = $RunId
        level      = $Level
        event_type = $EventType
        iuid       = $Iuid
        message    = $Message
        extra      = $Extra
    }

    if (Test-Path $EventFile) {
        $row | Export-Csv -Path $EventFile -NoTypeInformation -Encoding utf8 -Append
    } else {
        $row | Export-Csv -Path $EventFile -NoTypeInformation -Encoding utf8
    }
}

# 2. Preparacao de caminhos
if ([string]::IsNullOrWhiteSpace($runId)) {
    Write-Log "ERROR" "Defina a variavel runId antes de executar."
    exit 1
}

$scriptDir = Get-ScriptDirectory
$resolvedRunsBase = $null
if ([string]::IsNullOrWhiteSpace($runsBaseDir)) {
    $resolvedRunsBase = Join-Path $scriptDir "runs"
} elseif ([System.IO.Path]::IsPathRooted($runsBaseDir)) {
    $resolvedRunsBase = $runsBaseDir
} else {
    $resolvedRunsBase = Join-Path $scriptDir $runsBaseDir
}

$runDir = Join-Path $resolvedRunsBase $runId
if (-not (Test-Path $runDir)) {
    Write-Log "ERROR" "Run nao encontrado: $runDir"
    exit 1
}

$arquivoSucesso = Join-Path $runDir "sucesso_iuids.txt"
$arquivoErro = Join-Path $runDir "erro_iuids.txt"
$arquivoValidacao = Join-Path $runDir "validation_report.csv"
$arquivoEventos = Join-Path $runDir "validation_events.csv"
$arquivoReconciliacao = Join-Path $runDir "reconciliation_report.csv"
$arquivoNaoValidados = Join-Path $runDir "nao_validados_iuids.txt"

if (-not (Test-Path $arquivoSucesso)) {
    Write-Log "ERROR" "Arquivo de sucesso nao encontrado: $arquivoSucesso"
    exit 1
}

if (Test-Path $arquivoValidacao) { Remove-Item $arquivoValidacao -Force -ErrorAction SilentlyContinue }
if (Test-Path $arquivoEventos) { Remove-Item $arquivoEventos -Force -ErrorAction SilentlyContinue }
if (Test-Path $arquivoReconciliacao) { Remove-Item $arquivoReconciliacao -Force -ErrorAction SilentlyContinue }
if (Test-Path $arquivoNaoValidados) { Remove-Item $arquivoNaoValidados -Force -ErrorAction SilentlyContinue }

# 3. Carga de dados da run
$iuidsSucesso = @(Get-Content $arquivoSucesso | Where-Object { $_ -ne "" } | Sort-Object -Unique)
$iuidsErroSend = @()
if (Test-Path $arquivoErro) {
    $iuidsErroSend = @(Get-Content $arquivoErro | Where-Object { $_ -ne "" } | Sort-Object -Unique)
}

Write-Log "INFO" "Iniciando validacao do run: $runId"
Write-Log "INFO" "IUIDs de sucesso para validar: $($iuidsSucesso.Count)"
Write-ValidationEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "VALIDATION_START" -Message "Inicio da validacao por IUID."

# 4. Validacao sem retry (um request por IUID)
$okCount = 0
$missingCount = 0
$apiErrorCount = 0
$naoValidados = New-Object System.Collections.Generic.List[string]

foreach ($id in $iuidsSucesso) {
    $url = "http://$ipServer/dcm4chee-arc/aets/$aet/rs/instances?SOPInstanceUID=$id"
    $status = "NOT_FOUND"
    $detalhe = ""

    try {
        $resp = Invoke-RestMethod -Uri $url -Method Get -ErrorAction Stop
        if ($resp -and $resp.Count -gt 0) {
            $status = "OK"
            $okCount++
        } else {
            $status = "NOT_FOUND"
            $missingCount++
            $naoValidados.Add($id) | Out-Null
        }
    } catch {
        $status = "API_ERROR"
        $detalhe = $_.Exception.Message
        $apiErrorCount++
        $naoValidados.Add($id) | Out-Null
    }

    [PSCustomObject]@{
        run_id         = $runId
        iuid           = $id
        status         = $status
        checked_at     = (Get-Date).ToString("s")
        api_detail     = $detalhe
    } | Export-Csv -Path $arquivoValidacao -NoTypeInformation -Encoding utf8 -Append

    Write-ValidationEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "IUID_VALIDATED" -Message "IUID validado." -Iuid $id -Extra "status=$status"
}

if ($naoValidados.Count -gt 0) {
    $naoValidados | Sort-Object -Unique | Set-Content -Path $arquivoNaoValidados -Encoding utf8
}

# 5. Reconciliacao final (sem retry)
$totalSucesso = $iuidsSucesso.Count
$totalErroSend = $iuidsErroSend.Count
$statusFinal = "PASS"
$motivo = "Todos os IUIDs de sucesso foram encontrados no dcm4chee."

if ($totalSucesso -gt 0 -and $apiErrorCount -eq $totalSucesso) {
    $statusFinal = "FAIL"
    $motivo = "Falha total de consulta na API durante a validacao."
} elseif ($missingCount -gt 0 -or $apiErrorCount -gt 0 -or $totalErroSend -gt 0) {
    $statusFinal = "PASS_WITH_WARNINGS"
    $motivo = "Existem IUIDs nao validados e/ou erros do envio."
}

[PSCustomObject]@{
    run_id                    = $runId
    total_iuids_sucesso       = $totalSucesso
    total_iuids_ok            = $okCount
    total_iuids_not_found     = $missingCount
    total_iuids_api_error     = $apiErrorCount
    total_iuids_erro_send     = $totalErroSend
    final_status              = $statusFinal
    reason                    = $motivo
    generated_at              = (Get-Date).ToString("s")
} | Export-Csv -Path $arquivoReconciliacao -NoTypeInformation -Encoding utf8

Write-ValidationEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "RECONCILIATION_END" -Message "Reconciliação final gerada." -Extra "final_status=$statusFinal"
Write-ValidationEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "VALIDATION_END" -Message "Validacao finalizada."

Write-Host "`n--- Relatorio Final (VALIDACAO) ---" -ForegroundColor Yellow
Write-Host "Run ID:                    $runId"
Write-Host "IUIDs sucesso (entrada):   $totalSucesso"
Write-Host "IUIDs OK:                  $okCount"
Write-Host "IUIDs NOT_FOUND:           $missingCount"
Write-Host "IUIDs API_ERROR:           $apiErrorCount"
Write-Host "IUIDs erro no send:        $totalErroSend"
Write-Host "Status final:              $statusFinal"
Write-Host "Relatorio validacao CSV:   $arquivoValidacao"
Write-Host "Reconciliacao CSV:         $arquivoReconciliacao"
