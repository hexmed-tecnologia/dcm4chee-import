# 1. Configuracoes
$dcm4cheBinPath = "C:\Users\Administrator\Desktop\dcm4che-5.34.2-bin\dcm4che-5.34.2\bin"
$pastaExames = "E:\TEMP"
$arquivoSucesso = "sucesso_iuids.txt"
$arquivoErro = "erro_iuids.txt"
$arquivoLog = "" # Opcional. Se vazio, usa "storescu_execucao.log" no diretorio do script.
$mostrarOutputEmTempoReal = $true
$nivelLogMinimo = "INFO" # DEBUG, INFO, WARN, ERROR

$aetDestino = "HMD_IMPORTED"
$ipPacs = "192.168.1.70"
$portaPacs = 5555

# 2. Funcoes de suporte
$ordemNivelLog = @{
    DEBUG = 10
    INFO  = 20
    WARN  = 30
    ERROR = 40
}

function Write-Log {
    param(
        [string]$Nivel,
        [string]$Mensagem
    )

    if (-not $ordemNivelLog.ContainsKey($Nivel)) {
        return
    }

    if ($ordemNivelLog[$Nivel] -lt $ordemNivelLog[$nivelLogMinimo]) {
        return
    }

    $cor = "White"
    switch ($Nivel) {
        "DEBUG" { $cor = "DarkGray" }
        "INFO"  { $cor = "Cyan" }
        "WARN"  { $cor = "Yellow" }
        "ERROR" { $cor = "Red" }
    }

    Write-Host "[$Nivel] $Mensagem" -ForegroundColor $cor
}

# 3. Validacoes de entrada
$storescuPath = Join-Path $dcm4cheBinPath "storescu.bat"
if (-not (Test-Path $storescuPath)) {
    Write-Log "ERROR" "storescu.bat nao encontrado em: $storescuPath"
    exit 1
}

if (-not (Test-Path $pastaExames)) {
    Write-Log "ERROR" "Pasta de exames nao encontrada: $pastaExames"
    exit 1
}

$arquivoLogCompleto = $null
$scriptDir = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($scriptDir)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($scriptDir)) {
    $scriptDir = (Get-Location).Path
}

if ([string]::IsNullOrWhiteSpace($arquivoLog)) {
    $arquivoLogCompleto = Join-Path $scriptDir "storescu_execucao.log"
} elseif ([System.IO.Path]::IsPathRooted($arquivoLog)) {
    $arquivoLogCompleto = $arquivoLog
} else {
    $arquivoLogCompleto = Join-Path $scriptDir $arquivoLog
}

$logDir = Split-Path -Parent $arquivoLogCompleto
if (-not [string]::IsNullOrWhiteSpace($logDir) -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

# 4. Montagem dos argumentos e execucao
$storescuArgs = @(
    "-c", "${aetDestino}@${ipPacs}:$portaPacs",
    $pastaExames
)

Write-Log "INFO" "Iniciando envio para $aetDestino..."
if ($arquivoLogCompleto) {
    Write-Log "INFO" "Log de execucao sera salvo em: $arquivoLogCompleto"
}
Write-Log "DEBUG" "storescu path: $storescuPath"
Write-Log "DEBUG" "storescu args: $($storescuArgs -join ' ')"

$linhas = @()
if ($mostrarOutputEmTempoReal) {
    $linhas = & $storescuPath @storescuArgs 2>&1 | Tee-Object -FilePath $arquivoLogCompleto
} else {
    $linhas = & $storescuPath @storescuArgs 2>&1 | Tee-Object -FilePath $arquivoLogCompleto
    $linhas | Out-Null
}

$exitCode = $LASTEXITCODE
if ($null -eq $exitCode -or $exitCode -eq "") {
    $exitCode = -1
}
if ($exitCode -ne 0) {
    Write-Log "WARN" "storescu finalizou com exit code $exitCode (validando mesmo assim pelo log DICOM)."
}

$resultado = ($linhas -join [Environment]::NewLine)

# 5. Filtragem por Regex
$regexSucesso = "status=0H[\s\S]*?iuid=([\d\.]+)"
$regexErro = "status=[^0][A-F0-9]*H[\s\S]*?iuid=([\d\.]+)"

$sucessos = [regex]::Matches($resultado, $regexSucesso, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
    ForEach-Object { $_.Groups[1].Value }
$erros = [regex]::Matches($resultado, $regexErro, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
    ForEach-Object { $_.Groups[1].Value }

# 6. Persistencia dos resultados
$sucessos | Out-File -FilePath $arquivoSucesso -Encoding utf8
$erros | Out-File -FilePath $arquivoErro -Encoding utf8

# 7. Resumo final
Write-Host "`n--- Relatorio Final ---" -ForegroundColor Yellow
Write-Host "Enviados com Sucesso: $($sucessos.Count) (Salvos em $arquivoSucesso)"
Write-Host "Falhas detectadas:    $($erros.Count) (Salvas em $arquivoErro)"
if ($arquivoLogCompleto) {
    Write-Host "Log completo:         $arquivoLogCompleto"
}