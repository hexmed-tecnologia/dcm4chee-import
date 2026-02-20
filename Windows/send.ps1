# 1. Configuracoes
$dcm4cheBinPath = "C:\Users\Administrator\Desktop\dcm4che-5.34.2-bin\dcm4che-5.34.2\bin"
$pastaExames = "E:\TEMP"
$arquivoSucesso = "sucesso_iuids.txt"
$arquivoErro = "erro_iuids.txt"
$arquivoLog = "" # Opcional. Se vazio, usa "storescu_execucao.log" no diretorio do script.
$arquivoCheckpoint = "send_checkpoint.json" # Sempre salvo no diretorio do script.
$retomarDeCheckpoint = $true
$tamanhoBatchPastas = 50
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

function New-StringSet {
    return New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
}

function Get-LeafDirectories {
    param([string]$RootPath)

    $leafDirs = New-Object System.Collections.Generic.List[string]
    $rootItem = Get-Item -LiteralPath $RootPath -ErrorAction Stop
    $stack = New-Object 'System.Collections.Generic.Stack[System.IO.DirectoryInfo]'
    $stack.Push($rootItem)

    while ($stack.Count -gt 0) {
        $current = $stack.Pop()
        $subdirs = @(Get-ChildItem -LiteralPath $current.FullName -Directory -ErrorAction SilentlyContinue)

        if ($subdirs.Count -eq 0) {
            $leafDirs.Add($current.FullName)
            continue
        }

        foreach ($subdir in $subdirs) {
            $stack.Push($subdir)
        }
    }

    return $leafDirs
}

function Save-Checkpoint {
    param(
        [string]$CheckpointPath,
        [string]$RootPath,
        [int]$BatchSize,
        [System.Collections.Generic.HashSet[string]]$CompletedSet
    )

    $checkpointObj = [PSCustomObject]@{
        rootPath         = $RootPath
        batchSize        = $BatchSize
        updatedAt        = (Get-Date).ToString("s")
        completedFolders = @($CompletedSet)
    }

    $checkpointObj | ConvertTo-Json -Depth 5 | Set-Content -Path $CheckpointPath -Encoding utf8
}

function Append-NewIuids {
    param(
        [string]$TextoLog,
        [string]$ArquivoDestino,
        [string]$RegexPadrao,
        [System.Collections.Generic.HashSet[string]]$SetDestino
    )

    $matches = [regex]::Matches($TextoLog, $RegexPadrao, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    foreach ($match in $matches) {
        $iuid = $match.Groups[1].Value
        if (-not [string]::IsNullOrWhiteSpace($iuid) -and $SetDestino.Add($iuid)) {
            Add-Content -Path $ArquivoDestino -Value $iuid -Encoding utf8
        }
    }
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

if ($tamanhoBatchPastas -lt 1) {
    Write-Log "ERROR" "tamanhoBatchPastas deve ser >= 1."
    exit 1
}

$scriptDir = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($scriptDir)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($scriptDir)) {
    $scriptDir = (Get-Location).Path
}

$arquivoLogCompleto = $null
if ([string]::IsNullOrWhiteSpace($arquivoLog)) {
    $arquivoLogCompleto = Join-Path $scriptDir "storescu_execucao.log"
} elseif ([System.IO.Path]::IsPathRooted($arquivoLog)) {
    $arquivoLogCompleto = $arquivoLog
} else {
    $arquivoLogCompleto = Join-Path $scriptDir $arquivoLog
}

$arquivoCheckpointCompleto = Join-Path $scriptDir $arquivoCheckpoint

$logDir = Split-Path -Parent $arquivoLogCompleto
if (-not [string]::IsNullOrWhiteSpace($logDir) -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

# 4. Estado de execucao e checkpoint
$pastasConcluidas = New-StringSet
$sucessoSet = New-StringSet
$erroSet = New-StringSet
$checkpointValido = $false

if ($retomarDeCheckpoint -and (Test-Path $arquivoCheckpointCompleto)) {
    try {
        $checkpoint = Get-Content -Path $arquivoCheckpointCompleto -Raw | ConvertFrom-Json
        if ($checkpoint -and $checkpoint.rootPath -eq $pastaExames) {
            foreach ($p in @($checkpoint.completedFolders)) {
                if (-not [string]::IsNullOrWhiteSpace($p)) {
                    [void]$pastasConcluidas.Add($p)
                }
            }
            $checkpointValido = $true
            Write-Log "INFO" "Checkpoint carregado: $($pastasConcluidas.Count) pastas concluidas."
        } else {
            Write-Log "WARN" "Checkpoint ignorado: rootPath diferente da configurada."
        }
    } catch {
        Write-Log "WARN" "Falha ao ler checkpoint existente. Sera iniciada uma nova execucao."
    }
}

if (-not $checkpointValido) {
    New-Item -ItemType File -Path $arquivoLogCompleto -Force | Out-Null
    Clear-Content -Path $arquivoLogCompleto -ErrorAction SilentlyContinue

    New-Item -ItemType File -Path $arquivoSucesso -Force | Out-Null
    Clear-Content -Path $arquivoSucesso -ErrorAction SilentlyContinue

    New-Item -ItemType File -Path $arquivoErro -Force | Out-Null
    Clear-Content -Path $arquivoErro -ErrorAction SilentlyContinue

    if (Test-Path $arquivoCheckpointCompleto) {
        Remove-Item -Path $arquivoCheckpointCompleto -Force -ErrorAction SilentlyContinue
    }
} else {
    if (Test-Path $arquivoSucesso) {
        foreach ($id in (Get-Content -Path $arquivoSucesso | Where-Object { $_ -ne "" })) {
            [void]$sucessoSet.Add($id)
        }
    } else {
        New-Item -ItemType File -Path $arquivoSucesso -Force | Out-Null
    }

    if (Test-Path $arquivoErro) {
        foreach ($id in (Get-Content -Path $arquivoErro | Where-Object { $_ -ne "" })) {
            [void]$erroSet.Add($id)
        }
    } else {
        New-Item -ItemType File -Path $arquivoErro -Force | Out-Null
    }
}

Write-Log "INFO" "Iniciando envio para $aetDestino..."
Write-Log "INFO" "Log de execucao: $arquivoLogCompleto"
Write-Log "INFO" "Checkpoint: $arquivoCheckpointCompleto"
Write-Log "INFO" "Batch de pastas-fim: $tamanhoBatchPastas"
Write-Log "INFO" "Para cancelar: Ctrl+C. Emergencia: taskkill /F /T /IM java.exe"
Write-Log "DEBUG" "storescu path: $storescuPath"

# 5. Descoberta das pastas-fim (sem detectar tipo DICOM)
$leafDirs = @(Get-LeafDirectories -RootPath $pastaExames)
Write-Log "INFO" "Pastas-fim encontradas: $($leafDirs.Count)"

$pendentes = @($leafDirs | Where-Object { -not $pastasConcluidas.Contains($_) })
Write-Log "INFO" "Pastas pendentes para envio: $($pendentes.Count)"

if ($pendentes.Count -eq 0) {
    Write-Log "INFO" "Nada a enviar. Todas as pastas-fim ja foram concluidas."
    exit 0
}

$totalBatches = [Math]::Ceiling($pendentes.Count / [double]$tamanhoBatchPastas)
$regexSucesso = "status=0H[\s\S]*?iuid=([\d\.]+)"
$regexErro = "status=[^0][A-F0-9]*H[\s\S]*?iuid=([\d\.]+)"
$pastasProcessadas = 0
$pastasVazias = 0

for ($offset = 0; $offset -lt $pendentes.Count; $offset += $tamanhoBatchPastas) {
    $batchNumero = [int]($offset / $tamanhoBatchPastas) + 1
    $fim = [Math]::Min($offset + $tamanhoBatchPastas - 1, $pendentes.Count - 1)
    $batch = $pendentes[$offset..$fim]
    Write-Log "INFO" "Iniciando batch $batchNumero/$totalBatches com $($batch.Count) pastas."

    foreach ($pastaLeaf in $batch) {
        $temArquivo = @(
            Get-ChildItem -LiteralPath $pastaLeaf -File -ErrorAction SilentlyContinue | Select-Object -First 1
        ).Count -gt 0

        if (-not $temArquivo) {
            $pastasVazias++
            Write-Log "INFO" "[SKIPPED_EMPTY] $pastaLeaf"
            [void]$pastasConcluidas.Add($pastaLeaf)
            Save-Checkpoint -CheckpointPath $arquivoCheckpointCompleto -RootPath $pastaExames -BatchSize $tamanhoBatchPastas -CompletedSet $pastasConcluidas
            continue
        }

        $storescuArgs = @(
            "-c", "${aetDestino}@${ipPacs}:$portaPacs",
            $pastaLeaf
        )

        Write-Log "INFO" "[SEND] Pasta: $pastaLeaf"
        $linhas = @()
        if ($mostrarOutputEmTempoReal) {
            $linhas = & $storescuPath @storescuArgs 2>&1 | Tee-Object -FilePath $arquivoLogCompleto -Append
        } else {
            $linhas = & $storescuPath @storescuArgs 2>&1 | Tee-Object -FilePath $arquivoLogCompleto -Append
            $linhas | Out-Null
        }

        $exitCode = $LASTEXITCODE
        if ($null -eq $exitCode -or $exitCode -eq "") {
            $exitCode = -1
        }
        if ($exitCode -ne 0) {
            Write-Log "WARN" "storescu finalizou com exit code $exitCode para a pasta: $pastaLeaf"
        }

        $resultadoLote = ($linhas | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
        if (-not [string]::IsNullOrWhiteSpace($resultadoLote)) {
            Append-NewIuids -TextoLog $resultadoLote -ArquivoDestino $arquivoSucesso -RegexPadrao $regexSucesso -SetDestino $sucessoSet
            Append-NewIuids -TextoLog $resultadoLote -ArquivoDestino $arquivoErro -RegexPadrao $regexErro -SetDestino $erroSet
        }

        $pastasProcessadas++
        [void]$pastasConcluidas.Add($pastaLeaf)
        Save-Checkpoint -CheckpointPath $arquivoCheckpointCompleto -RootPath $pastaExames -BatchSize $tamanhoBatchPastas -CompletedSet $pastasConcluidas
    }
}

# 6. Resumo final
Write-Host "`n--- Relatorio Final ---" -ForegroundColor Yellow
Write-Host "Pastas-fim processadas: $pastasProcessadas"
Write-Host "Pastas-fim vazias:      $pastasVazias (SKIPPED_EMPTY)"
Write-Host "IUIDs sucesso:          $($sucessoSet.Count) (Salvos em $arquivoSucesso)"
Write-Host "IUIDs erro:             $($erroSet.Count) (Salvos em $arquivoErro)"
Write-Host "Log completo:           $arquivoLogCompleto"
Write-Host "Checkpoint:             $arquivoCheckpointCompleto"