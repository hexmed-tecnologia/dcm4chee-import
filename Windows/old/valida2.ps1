# 1. Configurações
$arquivoEntrada = "sucesso_iuids.txt"
$arquivoRelatorio = "relatorio_por_exame.csv"
$aet = "HMD_IMPORTED"
$ipServer = "192.168.1.70:8080"
$todosOsItens = New-Object System.Collections.Generic.List[PSObject]

if (-not (Test-Path $arquivoEntrada)) {
    Write-Host "Erro: Arquivo $arquivoEntrada não encontrado!" -ForegroundColor Red
    exit
}

$iuids = Get-Content $arquivoEntrada | Where-Object { $_ -ne "" }
Write-Host "Consultando $($iuids.Count) instâncias no dcm4chee..." -ForegroundColor Cyan

# 2. Coleta de Dados (Loop Principal)
foreach ($id in $iuids) {
    $url = "http://$ipServer/dcm4chee-arc/aets/$aet/rs/instances?SOPInstanceUID=$id"
    try {
        $resp = Invoke-RestMethod -Uri $url -Method Get -ErrorAction Stop
        if ($resp -and $resp.Count -gt 0) {
            $item = $resp[0]
            # Mapeamento incluindo StudyUID (0020000D)
            $obj = [PSCustomObject]@{
                Status      = "✅ OK"
                Paciente    = $item."00100010".Value[0].Alphabetic
                Prontuario  = $item."00100020".Value[0]
                DataExame   = $item."00080020".Value[0]
                StudyUID    = $item."0020000D".Value[0]
                IUID        = $id
            }
            $todosOsItens.Add($obj)
        }
    } catch {
        Write-Host "[ERRO] ID não localizado: $id" -ForegroundColor Red
    }
}

# 3. Agrupamento e Contagem
# Agrupamos pelo StudyUID para consolidar o exame
$examesAgrupados = $todosOsItens | Group-Object StudyUID | ForEach-Object {
    $primeiroItem = $_.Group[0]
    [PSCustomObject]@{
        Status       = $primeiroItem.Status
        Paciente     = $primeiroItem.Paciente
        Prontuario   = $primeiroItem.Prontuario
        DataExame    = $primeiroItem.DataExame
        QtdImagens   = $_.Count  # Conta quantos IUIDs existem neste grupo
        StudyUID     = $_.Name
    }
}

# 4. Exportação e Exibição
$examesAgrupados | Export-Csv -Path $arquivoRelatorio -NoTypeInformation -Encoding utf8
Write-Host "`n--- Resumo por Exame (Study) ---" -ForegroundColor Yellow
$examesAgrupados | Format-Table -Property Status, Paciente, QtdImagens, DataExame, StudyUID -AutoSize

Write-Host "Relatório consolidado salvo em: $arquivoRelatorio" -ForegroundColor Cyan