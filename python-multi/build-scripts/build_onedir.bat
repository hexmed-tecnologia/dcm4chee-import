@echo off
setlocal enableextensions enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "APP_FILE=%PROJECT_DIR%\app.py"
set "VERSION_FILE=%PROJECT_DIR%\VERSION"
set "APP_NAME=DicomMultiToolkit"
set "TOOLKITS_DIR=%PROJECT_DIR%\toolkits"

set "ARTIFACTS_DIR=%SCRIPT_DIR%artifacts"
set "VENV_DIR=%ARTIFACTS_DIR%\.venv-build"
set "DIST_ROOT=%SCRIPT_DIR%dist"
set "BUILD_DIR=%ARTIFACTS_DIR%\build"
set "SPEC_DIR=%ARTIFACTS_DIR%\spec"

if not exist "%ARTIFACTS_DIR%" mkdir "%ARTIFACTS_DIR%"
if not exist "%DIST_ROOT%" mkdir "%DIST_ROOT%"
if not exist "%VERSION_FILE%" (
  echo [ERROR] Arquivo VERSION nao encontrado: %VERSION_FILE%
  exit /b 1
)
set /p APP_VERSION=<"%VERSION_FILE%"
if "%APP_VERSION%"=="" (
  echo [ERROR] Arquivo VERSION vazio: %VERSION_FILE%
  exit /b 1
)
for /f "tokens=*" %%v in ("%APP_VERSION%") do set "APP_VERSION=%%v"
set "OUTPUT_NAME=%APP_NAME%-%APP_VERSION%"

py -3.12 -V >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Python 3.12 nao encontrado no launcher py.
  exit /b 1
)

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [INFO] Criando venv...
  py -3.12 -m venv "%VENV_DIR%"
  if errorlevel 1 exit /b 1
)

call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 exit /b 1

python -m pip install --upgrade pip >nul
python -m pip install pyinstaller >nul

if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
if exist "%SPEC_DIR%" rmdir /s /q "%SPEC_DIR%"
mkdir "%SPEC_DIR%"

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"ddMMyyyy_HHmmss\")"') do set "BUILD_STAMP=%%i"
set "DIST_DIR=%DIST_ROOT%\%BUILD_STAMP%"
mkdir "%DIST_DIR%"

echo [INFO] Build timestamp: %BUILD_STAMP%
echo [INFO] App version: %APP_VERSION%
echo [INFO] Output folder name: %OUTPUT_NAME%
echo [INFO] Dist output: %DIST_DIR%

for /f %%a in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeSeconds()"') do set "BUILD_START=%%a"

if exist "%TOOLKITS_DIR%" (
  echo [INFO] Including toolkits folder in build: %TOOLKITS_DIR%
  python -m PyInstaller ^
    --noconfirm ^
    --onedir ^
    --windowed ^
    --name "%OUTPUT_NAME%" ^
    --add-data "%VERSION_FILE%;." ^
    --add-data "%TOOLKITS_DIR%;toolkits" ^
    --distpath "%DIST_DIR%" ^
    --workpath "%BUILD_DIR%" ^
    --specpath "%SPEC_DIR%" ^
    "%APP_FILE%"
) else (
  echo [WARN] Toolkits folder not found. Build will not embed toolkits.
  python -m PyInstaller ^
    --noconfirm ^
    --onedir ^
    --windowed ^
    --name "%OUTPUT_NAME%" ^
    --add-data "%VERSION_FILE%;." ^
    --distpath "%DIST_DIR%" ^
    --workpath "%BUILD_DIR%" ^
    --specpath "%SPEC_DIR%" ^
    "%APP_FILE%"
)
if errorlevel 1 (
  echo [ERROR] PyInstaller build failed.
  exit /b 1
)

for /f %%a in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeSeconds()"') do set "BUILD_END=%%a"
set /a BUILD_DURATION=BUILD_END-BUILD_START

set "ZIP_SRC=%DIST_DIR%\%OUTPUT_NAME%"
set "ZIP_DST=%DIST_DIR%\%OUTPUT_NAME%.zip"
if exist "%ZIP_DST%" del /f /q "%ZIP_DST%" >nul 2>&1

for /f %%a in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeSeconds()"') do set "ZIP_START=%%a"

set "ZIP_TOOL="
for %%T in (nanazipc.exe 7z.exe 7za.exe) do (
  where %%T >nul 2>&1
  if not errorlevel 1 if not defined ZIP_TOOL set "ZIP_TOOL=%%T"
)

if defined ZIP_TOOL (
  echo [INFO] Zip engine: %ZIP_TOOL%
  goto :zip_with_7z
) else (
  echo [INFO] NanaZip/7z nao encontrado. Usando fallback PowerShell...
  call :zip_with_powershell "%ZIP_SRC%" "%ZIP_DST%"
)

goto :zip_done

:zip_with_7z
pushd "%DIST_DIR%"
"%ZIP_TOOL%" a -tzip "%OUTPUT_NAME%.zip" "%OUTPUT_NAME%" -bsp1 -bso1
set "ZIP_EXIT=%ERRORLEVEL%"
popd
if "%ZIP_EXIT%"=="0" (
  echo [INFO] Zip criado: %ZIP_DST%
) else (
  echo [WARN] %ZIP_TOOL% falhou. Usando fallback PowerShell...
  call :zip_with_powershell "%ZIP_SRC%" "%ZIP_DST%"
)

:zip_done

for /f %%a in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeSeconds()"') do set "ZIP_END=%%a"
set /a ZIP_DURATION=ZIP_END-ZIP_START

if errorlevel 1 (
  echo [WARN] Falha ao criar .zip apos todas as tentativas.
)

echo.
echo [INFO] Build concluido.
powershell -NoProfile -Command "$b=%BUILD_DURATION%; $z=%ZIP_DURATION%; function f($s){$m=[int]($s/60);$r=$s%%60;if($m -gt 0){\"$m min $r s\"}else{\"$r s\"}}; Write-Host '[INFO] Tempo - Build:' (f($b)) ', Zip:' (f($z))"
echo [INFO] Pasta:
echo        "%DIST_DIR%\%OUTPUT_NAME%"
echo [INFO] Executavel:
echo        "%DIST_DIR%\%OUTPUT_NAME%\%OUTPUT_NAME%.exe"
echo [INFO] Zip:
echo        "%DIST_DIR%\%OUTPUT_NAME%.zip"
echo.

endlocal
exit /b 0

:zip_with_powershell
powershell -NoProfile -Command "$src='%~1'; $dst='%~2'; $ok=$false; for($i=1; $i -le 6 -and -not $ok; $i++){ try { Compress-Archive -LiteralPath $src -DestinationPath $dst -Force -ErrorAction Stop; $ok=$true } catch { if($i -lt 6){ Write-Host ('[WARN] Tentativa ' + $i + ' falhou (arquivo em uso). Novo retry em 2s...'); Start-Sleep -Seconds 2 } } }; if($ok){ exit 0 } else { exit 1 }"
exit /b %ERRORLEVEL%

