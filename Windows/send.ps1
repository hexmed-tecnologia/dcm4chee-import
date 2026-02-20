# 1. Configuracoes principais
$dcm4cheBinPath = "C:\Users\Administrator\Desktop\dcm4che-5.34.2-bin\dcm4che-5.34.2\bin"
$pastaExames = "E:\TEMP"
$aetDestino = "HMD_IMPORTED"
$ipPacs = "192.168.1.70"
$portaPacs = 5555

# 2. Controle de execucao (run)
$runIdParaRetomar = "" # Exemplo: "20260220_101530". Se vazio, cria novo run.
$runsBaseDir = "" # Se vazio, usa "<diretorio_do_script>\runs"
$retomarDeCheckpoint = $true
$tamanhoBatchPastas = 50
$mostrarOutputEmTempoReal = $true

# 3. Logs e verbosidade
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

function New-StringSet {
    return New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
}

function Write-RunEvent {
    param(
        [string]$EventFile,
        [string]$RunId,
        [string]$Level,
        [string]$EventType,
        [string]$Message,
        [string]$Batch = "",
        [string]$FolderPath = "",
        [string]$Extra = ""
    )

    $row = [PSCustomObject]@{
        timestamp  = (Get-Date).ToString("s")
        run_id     = $RunId
        level      = $Level
        event_type = $EventType
        batch      = $Batch
        folder     = $FolderPath
        message    = $Message
        extra      = $Extra
    }

    if (Test-Path $EventFile) {
        $row | Export-Csv -Path $EventFile -NoTypeInformation -Encoding utf8 -Append
    } else {
        $row | Export-Csv -Path $EventFile -NoTypeInformation -Encoding utf8
    }
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
        } else {
            foreach ($sub in $subdirs) {
                $stack.Push($sub)
            }
        }
    }
    return $leafDirs
}

function Save-Checkpoint {
    param(
        [string]$CheckpointFile,
        [string]$RunId,
        [string]$RootPath,
        [int]$BatchSize,
        [System.Collections.Generic.HashSet[string]]$CompletedFolders
    )

    $payload = [PSCustomObject]@{
        runId            = $RunId
        rootPath         = $RootPath
        batchSize        = $BatchSize
        updatedAt        = (Get-Date).ToString("s")
        completedFolders = @($CompletedFolders)
    }

    $payload | ConvertTo-Json -Depth 6 | Set-Content -Path $CheckpointFile -Encoding utf8
}

function Append-NewIuids {
    param(
        [string]$TextLog,
        [string]$TargetFile,
        [string]$RegexPattern,
        [System.Collections.Generic.HashSet[string]]$TargetSet
    )

    $added = 0
    $matches = [regex]::Matches($TextLog, $RegexPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    foreach ($m in $matches) {
        $iuid = $m.Groups[1].Value
        if (-not [string]::IsNullOrWhiteSpace($iuid) -and $TargetSet.Add($iuid)) {
            Add-Content -Path $TargetFile -Value $iuid -Encoding utf8
            $added++
        }
    }
    return $added
}

# 4. Validacoes iniciais
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

$scriptDir = Get-ScriptDirectory
$resolvedRunsBase = $null
if ([string]::IsNullOrWhiteSpace($runsBaseDir)) {
    $resolvedRunsBase = Join-Path $scriptDir "runs"
} elseif ([System.IO.Path]::IsPathRooted($runsBaseDir)) {
    $resolvedRunsBase = $runsBaseDir
} else {
    $resolvedRunsBase = Join-Path $scriptDir $runsBaseDir
}
New-Item -ItemType Directory -Path $resolvedRunsBase -Force | Out-Null

$runId = $runIdParaRetomar
if ([string]::IsNullOrWhiteSpace($runId)) {
    $runId = (Get-Date).ToString("yyyyMMdd_HHmmss")
}
$runDir = Join-Path $resolvedRunsBase $runId
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$arquivoLogCompleto = Join-Path $runDir "storescu_execucao.log"
$arquivoSucesso = Join-Path $runDir "sucesso_iuids.txt"
$arquivoErro = Join-Path $runDir "erro_iuids.txt"
$arquivoCheckpoint = Join-Path $runDir "send_checkpoint.json"
$arquivoManifesto = Join-Path $runDir "manifest_folders.csv"
$arquivoResultadosPastas = Join-Path $runDir "folder_results.csv"
$arquivoEventos = Join-Path $runDir "send_events.csv"
$arquivoResumo = Join-Path $runDir "send_summary.csv"

$regexSucesso = "status=0H[\s\S]*?iuid=([\d\.]+)"
$regexErro = "status=[^0][A-F0-9]*H[\s\S]*?iuid=([\d\.]+)"
$pastasConcluidas = New-StringSet
$sucessoSet = New-StringSet
$erroSet = New-StringSet
$retomadaAplicada = $false

if ($retomarDeCheckpoint -and (Test-Path $arquivoCheckpoint)) {
    try {
        $cp = Get-Content -Path $arquivoCheckpoint -Raw | ConvertFrom-Json
        if ($cp.rootPath -eq $pastaExames) {
            foreach ($p in @($cp.completedFolders)) {
                if (-not [string]::IsNullOrWhiteSpace($p)) { [void]$pastasConcluidas.Add($p) }
            }
            $retomadaAplicada = $true
        }
    } catch {
        Write-Log "WARN" "Checkpoint existente invalido. Run continuara como novo."
    }
}

if (-not $retomadaAplicada) {
    New-Item -ItemType File -Path $arquivoLogCompleto -Force | Out-Null
    New-Item -ItemType File -Path $arquivoSucesso -Force | Out-Null
    New-Item -ItemType File -Path $arquivoErro -Force | Out-Null
    Clear-Content -Path $arquivoLogCompleto -ErrorAction SilentlyContinue
    Clear-Content -Path $arquivoSucesso -ErrorAction SilentlyContinue
    Clear-Content -Path $arquivoErro -ErrorAction SilentlyContinue
    if (Test-Path $arquivoResultadosPastas) { Remove-Item $arquivoResultadosPastas -Force -ErrorAction SilentlyContinue }
    if (Test-Path $arquivoEventos) { Remove-Item $arquivoEventos -Force -ErrorAction SilentlyContinue }
    if (Test-Path $arquivoResumo) { Remove-Item $arquivoResumo -Force -ErrorAction SilentlyContinue }
} else {
    if (Test-Path $arquivoSucesso) {
        foreach ($id in Get-Content -Path $arquivoSucesso) { if ($id) { [void]$sucessoSet.Add($id) } }
    } else {
        New-Item -ItemType File -Path $arquivoSucesso -Force | Out-Null
    }
    if (Test-Path $arquivoErro) {
        foreach ($id in Get-Content -Path $arquivoErro) { if ($id) { [void]$erroSet.Add($id) } }
    } else {
        New-Item -ItemType File -Path $arquivoErro -Force | Out-Null
    }
}

Write-Log "INFO" "RUN_ID: $runId"
Write-Log "INFO" "Run dir: $runDir"
Write-Log "INFO" "Batch de pastas-fim: $tamanhoBatchPastas"
Write-Log "INFO" "Para cancelar: Ctrl+C. Emergencia: taskkill /F /T /IM java.exe"
Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "RUN_START" -Message "Inicio da execucao de envio." -Extra "run_dir=$runDir"

# 5. Descoberta de pastas-fim e manifesto (recalculo a cada execucao)
$leafDirs = @(Get-LeafDirectories -RootPath $pastaExames)
$manifestRows = New-Object System.Collections.Generic.List[object]
foreach ($dir in $leafDirs) {
    $qtdArquivos = @(Get-ChildItem -LiteralPath $dir -File -ErrorAction SilentlyContinue).Count
    $manifestRows.Add([PSCustomObject]@{
        run_id       = $runId
        folder_path  = $dir
        file_count   = $qtdArquivos
        discovered_at = (Get-Date).ToString("s")
    })
}
$manifestRows | Export-Csv -Path $arquivoManifesto -NoTypeInformation -Encoding utf8

Write-Log "INFO" "Pastas-fim encontradas: $($leafDirs.Count)"
Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "MANIFEST_READY" -Message "Manifesto gerado." -Extra "total_leaf_dirs=$($leafDirs.Count)"

$pendentes = @($leafDirs | Where-Object { -not $pastasConcluidas.Contains($_) })
Write-Log "INFO" "Pastas pendentes para envio: $($pendentes.Count)"

if ($pendentes.Count -eq 0) {
    Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "RUN_END" -Message "Nada pendente para envio."
    Write-Host "Nada pendente para envio neste run."
    exit 0
}

$totalBatches = [Math]::Ceiling($pendentes.Count / [double]$tamanhoBatchPastas)
$pastasEnviadas = 0
$pastasVazias = 0

for ($offset = 0; $offset -lt $pendentes.Count; $offset += $tamanhoBatchPastas) {
    $batchNumero = [int]($offset / $tamanhoBatchPastas) + 1
    $fim = [Math]::Min($offset + $tamanhoBatchPastas - 1, $pendentes.Count - 1)
    $batch = @($pendentes[$offset..$fim])

    Write-Log "INFO" "Batch $batchNumero/$totalBatches iniciado com $($batch.Count) pastas."
    Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "BATCH_START" -Message "Batch iniciado." -Batch $batchNumero -Extra "batch_size=$($batch.Count)"

    foreach ($pastaLeaf in $batch) {
        $temArquivo = @(Get-ChildItem -LiteralPath $pastaLeaf -File -ErrorAction SilentlyContinue | Select-Object -First 1).Count -gt 0

        if (-not $temArquivo) {
            $pastasVazias++
            [void]$pastasConcluidas.Add($pastaLeaf)
            Save-Checkpoint -CheckpointFile $arquivoCheckpoint -RunId $runId -RootPath $pastaExames -BatchSize $tamanhoBatchPastas -CompletedFolders $pastasConcluidas
            Write-Log "INFO" "[SKIPPED_EMPTY] $pastaLeaf"
            Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "SKIPPED_EMPTY" -Message "Pasta sem arquivos." -Batch $batchNumero -FolderPath $pastaLeaf

            [PSCustomObject]@{
                run_id            = $runId
                folder_path       = $pastaLeaf
                batch             = $batchNumero
                status            = "SKIPPED_EMPTY"
                exit_code         = ""
                iuids_sucesso_novos = 0
                iuids_erro_novos    = 0
                processed_at      = (Get-Date).ToString("s")
            } | Export-Csv -Path $arquivoResultadosPastas -NoTypeInformation -Encoding utf8 -Append
            continue
        }

        Write-Log "INFO" "[SEND] $pastaLeaf"
        Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "FOLDER_SEND_START" -Message "Envio de pasta iniciado." -Batch $batchNumero -FolderPath $pastaLeaf

        $storescuArgs = @("-c", "${aetDestino}@${ipPacs}:$portaPacs", $pastaLeaf)
        $linhas = @()
        if ($mostrarOutputEmTempoReal) {
            $linhas = & $storescuPath @storescuArgs 2>&1 | Tee-Object -FilePath $arquivoLogCompleto -Append
        } else {
            $linhas = & $storescuPath @storescuArgs 2>&1 | Tee-Object -FilePath $arquivoLogCompleto -Append
            $linhas | Out-Null
        }

        $exitCode = $LASTEXITCODE
        if ($null -eq $exitCode -or $exitCode -eq "") { $exitCode = -1 }

        $textoLote = ($linhas | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
        $novosSucesso = 0
        $novosErro = 0
        if (-not [string]::IsNullOrWhiteSpace($textoLote)) {
            $novosSucesso = Append-NewIuids -TextLog $textoLote -TargetFile $arquivoSucesso -RegexPattern $regexSucesso -TargetSet $sucessoSet
            $novosErro = Append-NewIuids -TextLog $textoLote -TargetFile $arquivoErro -RegexPattern $regexErro -TargetSet $erroSet
        }

        $statusPasta = "DONE"
        if ($exitCode -ne 0) {
            $statusPasta = "DONE_WITH_WARNINGS"
            Write-Log "WARN" "storescu exit code $exitCode na pasta: $pastaLeaf"
        }

        [PSCustomObject]@{
            run_id              = $runId
            folder_path         = $pastaLeaf
            batch               = $batchNumero
            status              = $statusPasta
            exit_code           = $exitCode
            iuids_sucesso_novos = $novosSucesso
            iuids_erro_novos    = $novosErro
            processed_at        = (Get-Date).ToString("s")
        } | Export-Csv -Path $arquivoResultadosPastas -NoTypeInformation -Encoding utf8 -Append

        Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "FOLDER_SEND_END" -Message "Envio de pasta concluido." -Batch $batchNumero -FolderPath $pastaLeaf -Extra "status=$statusPasta;exit_code=$exitCode;new_success=$novosSucesso;new_error=$novosErro"

        $pastasEnviadas++
        [void]$pastasConcluidas.Add($pastaLeaf)
        Save-Checkpoint -CheckpointFile $arquivoCheckpoint -RunId $runId -RootPath $pastaExames -BatchSize $tamanhoBatchPastas -CompletedFolders $pastasConcluidas
    }

    Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "BATCH_END" -Message "Batch concluido." -Batch $batchNumero
}

$statusEnvio = "PASS"
if ($erroSet.Count -gt 0) {
    $statusEnvio = "PASS_WITH_WARNINGS"
}

[PSCustomObject]@{
    run_id                 = $runId
    root_path              = $pastaExames
    aet_destino            = $aetDestino
    batch_size             = $tamanhoBatchPastas
    leaf_dirs_total        = $leafDirs.Count
    folders_completed      = $pastasConcluidas.Count
    folders_sent           = $pastasEnviadas
    folders_skipped_empty  = $pastasVazias
    success_iuids_total    = $sucessoSet.Count
    error_iuids_total      = $erroSet.Count
    send_status            = $statusEnvio
    finished_at            = (Get-Date).ToString("s")
} | Export-Csv -Path $arquivoResumo -NoTypeInformation -Encoding utf8

Write-RunEvent -EventFile $arquivoEventos -RunId $runId -Level "INFO" -EventType "RUN_END" -Message "Execucao de envio finalizada." -Extra "send_status=$statusEnvio"

Write-Host "`n--- Relatorio Final (SEND) ---" -ForegroundColor Yellow
Write-Host "Run ID:                 $runId"
Write-Host "Run dir:                $runDir"
Write-Host "Pastas-fim totais:      $($leafDirs.Count)"
Write-Host "Pastas enviadas:        $pastasEnviadas"
Write-Host "Pastas SKIPPED_EMPTY:   $pastasVazias"
Write-Host "IUIDs sucesso:          $($sucessoSet.Count)"
Write-Host "IUIDs erro:             $($erroSet.Count)"
Write-Host "Status do send:         $statusEnvio"