# Configurações
$iuid = "1.2.392.200036.9116.3.1.19592149.20241026120728803.1022.2.4902"
$url = "http://192.168.1.70:8080/dcm4chee-arc/aets/HMD_IMPORTED/rs/instances?SOPInstanceUID=$iuid"

# Faz a consulta
$response = Invoke-RestMethod -Uri $url -Method Get

if ($response) {
    # Extrai os dados baseando-se nas tags DICOM
    $dados = [PSCustomObject]@{
        Paciente  = $response."00100010".Value.Alphabetic
        ID        = $response."00100020".Value
        DataExame = $response."00080020".Value
        Modalidade= $response."00080060".Value
        StudyUID  = $response."0020000D".Value
    }

    # Exibe no console
    Write-Host "--- Dados do Exame Encontrado ---" -ForegroundColor Cyan
    $dados | Format-List
} else {
    Write-Host "Nenhum exame encontrado para este IUID." -ForegroundColor Red
}