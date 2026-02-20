# 1. Configurações
$arquivoEntrada = "sucesso_iuids.txt"
$arquivoRelatorio = "relatorio_validacao.csv"
$aet = "HMD_IMPORTED"
$ipServer = "192.168.1.70:8080"
$listaRelatorio = New-Object System.Collections.Generic.List[PSObject]

# Verifica se o arquivo de entrada existe
if (-not (Test-Path $arquivoEntrada)) {
    Write-Host "Erro: Arquivo $arquivoEntrada não encontrado!" -ForegroundColor Red
    exit
}

$iuids = Get-Content $arquivoEntrada | Where-Object { $_ -ne "" }
Write-Host "Iniciando validação de $($iuids.Count) instâncias no dcm4chee..." -ForegroundColor Cyan

# 2. Loop de Consulta
foreach ($id in $iuids) {
    $url = "http://$ipServer/dcm4chee-arc/aets/$aet/rs/instances?SOPInstanceUID=$id"
    
    try {
        # Faz a chamada à API
        $resp = Invoke-RestMethod -Uri $url -Method Get -ErrorAction Stop
        
        if ($resp -and $resp.Count -gt 0) {
            # O dcm4chee retorna um array, pegamos o primeiro item
            $item = $resp[0]
            
            # Mapeia as tags DICOM para nomes legíveis
            $obj = [PSCustomObject]@{
                Status      = "✅ OK"
                Paciente    = $item."00100010".Value[0].Alphabetic
                Prontuario  = $item."00100020".Value[0]
                DataExame   = $item."00080020".Value[0]
                Modalidade  = $item."00080060".Value[0]
                IUID        = $id
            }
            Write-Host "[OK] Encontrado: $($obj.Paciente)" -ForegroundColor Green
        } else {
            throw "Não encontrado na base"
        }
    } catch {
        $obj = [PSCustomObject]@{
            Status      = "❌ FALHA"
            Paciente    = "N/A"
            Prontuario  = "N/A"
            DataExame   = "N/A"
            Modalidade  = "N/A"
            IUID        = $id
        }
        Write-Host "[ERRO] ID não localizado: $id" -ForegroundColor Red
    }
    $listaRelatorio.Add($obj)
}

# 3. Exportação e Exibição Final
$listaRelatorio | Export-Csv -Path $arquivoRelatorio -NoTypeInformation -Encoding utf8
Write-Host "`n--- Relatório Gerado ---" -ForegroundColor Yellow
$listaRelatorio | Format-Table -AutoSize
Write-Host "Relatório salvo em: $arquivoRelatorio" -ForegroundColor Cyan