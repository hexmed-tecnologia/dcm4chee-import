# Anexa zips de dist/ nas releases (tags) correspondentes.
# Usa apenas Git + PowerShell + API do GitHub (nao precisa do gh).
# Requer: token do GitHub (env GITHUB_TOKEN, -Token, ou arquivo .env na pasta do script).
# Uso: .\upload_zips_to_releases.ps1
#      Ou defina GITHUB_TOKEN no .env: GITHUB_TOKEN=ghp_xxx (arquivo .env nao vai pro git).

param(
    [string]$DistPath = "",
    [string[]]$Tags = @(),
    [string]$Token = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir
$RepoRoot = (git -C $ProjectDir rev-parse --show-toplevel 2>$null)
if (-not $RepoRoot) { $RepoRoot = $ProjectDir }
$DefaultDist = Join-Path $ScriptDir "dist"
$DistRoot = if ($DistPath) { $DistPath } else { $DefaultDist }

if (-not (Test-Path -LiteralPath $DistRoot -PathType Container)) {
    Write-Host "[ERROR] Pasta dist nao encontrada: $DistRoot"
    exit 1
}

# Token: parametro, env ou arquivo .env na pasta do script
$token = if ($Token) { $Token } else { $env:GITHUB_TOKEN }
if (-not $token) {
    $envFile = Join-Path $ScriptDir ".env"
    if (Test-Path -LiteralPath $envFile -PathType Leaf) {
        Get-Content -LiteralPath $envFile -Encoding UTF8 | ForEach-Object {
            if ($_ -match '^\s*([^#=]+)=(.*)$') {
                $key = $Matches[1].Trim()
                $val = $Matches[2].Trim().Trim('"').Trim("'")
                if ($key -eq "GITHUB_TOKEN") { $token = $val }
            }
        }
    }
}
if (-not $token) {
    Write-Host "[ERROR] Defina GITHUB_TOKEN ou use -Token 'seu_token'."
    Write-Host "        Ou crie build-scripts\.env com uma linha: GITHUB_TOKEN=ghp_seu_token"
    Write-Host "        (o arquivo .env nao e commitado - esta no .gitignore)."
    exit 1
}

# Owner/repo a partir do remote origin
$remoteUrl = git -C $RepoRoot config --get remote.origin.url 2>$null
if (-not $remoteUrl) {
    Write-Host "[ERROR] Nenhum remote.origin.url encontrado."
    exit 1
}
if ($remoteUrl -match '^https://github\.com/([^/]+)/([^/\.]+)') {
    $owner = $Matches[1]
    $repo = $Matches[2]
} elseif ($remoteUrl -match '^git@github\.com:([^/]+)/([^/\.]+)') {
    $owner = $Matches[1]
    $repo = $Matches[2]
} else {
    Write-Host "[ERROR] Nao foi possivel obter owner/repo de: $remoteUrl"
    exit 1
}

# Tags
if ($Tags.Count -eq 0) {
    $tagList = git -C $RepoRoot tag -l "v*" 2>$null
    if ($tagList) { $Tags = $tagList.Trim() -split "`n" }
}
if ($Tags.Count -eq 0) {
    Write-Host "[WARN] Nenhuma tag v* encontrada. Use -Tags v2.1.0,v2.2.0"
    exit 1
}

# Headers para API
$headers = @{
    "Authorization" = "Bearer $token"
    "Accept"        = "application/vnd.github+json"
    "X-GitHub-Api-Version" = "2022-11-28"
}

# Escaneia dist: **/DicomMultiToolkit-v*.zip
$zips = Get-ChildItem -LiteralPath $DistRoot -Recurse -Filter "DicomMultiToolkit-v*.zip" -File -ErrorAction SilentlyContinue
$byVersion = @{}
foreach ($f in $zips) {
    if ($f.Name -match "DicomMultiToolkit-(v[\d\.]+)\.zip$") {
        $ver = $Matches[1]
        if (-not $byVersion[$ver]) { $byVersion[$ver] = @() }
        $byVersion[$ver] += $f
    }
}

foreach ($tag in $Tags) {
    $tag = $tag.Trim()
    if (-not $byVersion[$tag]) {
        Write-Host "[SKIP] Nenhum zip para tag $tag em $DistRoot"
        continue
    }
    $latest = $byVersion[$tag] | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $path = $latest.FullName
    $fileName = $latest.Name

    Write-Host "[INFO] Anexando a $tag : $path"

    # Obter release pelo tag
    $uri = "https://api.github.com/repos/$owner/$repo/releases/tags/$tag"
    try {
        $release = Invoke-RestMethod -Uri $uri -Headers $headers -Method Get
    } catch {
        if ($_.Exception.Response.StatusCode -eq 404) {
            Write-Host "[INFO] Release nao existe. Criando release para tag $tag ..."
            $body = @{ tag_name = $tag; name = $tag; body = "" } | ConvertTo-Json
            $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$owner/$repo/releases" -Headers $headers -Method Post -Body $body -ContentType "application/json; charset=utf-8"
        } else {
            Write-Host "[ERROR] Falha ao obter/criar release: $_"
            exit 1
        }
    }

    # upload_url contem {?name,label}; usar para enviar o asset
    $uploadUrl = $release.upload_url -replace '\{\?name,label\}', "?name=$fileName"
    $headersUpload = @{
        "Authorization" = "Bearer $token"
        "Accept"        = "application/vnd.github+json"
        "Content-Type"  = "application/zip"
    }
    $bytes = [System.IO.File]::ReadAllBytes($path)
    try {
        Invoke-RestMethod -Uri $uploadUrl -Headers $headersUpload -Method Post -Body $bytes | Out-Null
    } catch {
        if ($_.Exception.Response.StatusCode -eq 422) {
            Write-Host "[WARN] Asset com mesmo nome ja existe (substitua pela interface ou apague e rode de novo)."
        } else {
            Write-Host "[ERROR] Falha ao enviar zip: $_"
            exit 1
        }
    }
    Write-Host "[OK] Anexado: $tag"
}

Write-Host "[INFO] Concluido."
