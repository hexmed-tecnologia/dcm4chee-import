@echo off
setlocal enableextensions

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
echo [INFO] Dist output: %DIST_DIR%

if exist "%TOOLKITS_DIR%" (
  echo [INFO] Including toolkits folder in build: %TOOLKITS_DIR%
  python -m PyInstaller ^
    --noconfirm ^
    --onedir ^
    --windowed ^
    --name "%APP_NAME%" ^
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
    --name "%APP_NAME%" ^
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

echo.
echo [INFO] Build concluido.
echo [INFO] Executavel:
echo        "%DIST_DIR%\%APP_NAME%\%APP_NAME%.exe"
echo.

endlocal
exit /b 0

