@echo off
setlocal enableextensions

REM ------------------------------------------------------------
REM Build script (onedir) for DICOM Sender/Validator MVP
REM Fixed Python version: 3.12 (via py launcher)
REM ------------------------------------------------------------

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "APP_FILE=%PROJECT_DIR%\app.py"
set "TOOLKIT_DIR=%PROJECT_DIR%\dcm4che-5.34.2"

set "ARTIFACTS_DIR=%SCRIPT_DIR%artifacts"
set "VENV_DIR=%ARTIFACTS_DIR%\.venv-build"
set "DIST_ROOT=%ARTIFACTS_DIR%\dist"
set "BUILD_DIR=%ARTIFACTS_DIR%\build"
set "SPEC_DIR=%ARTIFACTS_DIR%\spec"
set "SPEC_FILE=%SPEC_DIR%\app.spec"
set "APP_NAME=DicomSenderValidator"
set "BUILD_STAMP="
set "DIST_DIR="

echo.
echo [INFO] Starting onedir build...
echo [INFO] Project dir: %PROJECT_DIR%
echo [INFO] Artifacts dir: %ARTIFACTS_DIR%
echo.

if not exist "%APP_FILE%" (
  echo [ERROR] app.py not found: %APP_FILE%
  exit /b 1
)

if not exist "%TOOLKIT_DIR%" (
  echo [ERROR] Toolkit folder not found: %TOOLKIT_DIR%
  echo [ERROR] Expected folder: python-windows\dcm4che-5.34.2
  exit /b 1
)

echo [INFO] Checking Python 3.12...
py -3.12 -V >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Python 3.12 not found via "py -3.12".
  echo [ERROR] Install Python 3.12 or adjust this script.
  exit /b 1
)

if not exist "%ARTIFACTS_DIR%" mkdir "%ARTIFACTS_DIR%"
if not exist "%SPEC_DIR%" mkdir "%SPEC_DIR%"
if not exist "%DIST_ROOT%" mkdir "%DIST_ROOT%"

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [INFO] Creating venv: %VENV_DIR%
  py -3.12 -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    exit /b 1
  )
)

echo [INFO] Activating venv...
call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
  echo [ERROR] Failed to activate virtual environment.
  exit /b 1
)

echo [INFO] Installing/updating build dependencies...
python -m pip install --upgrade pip setuptools wheel pyinstaller
if errorlevel 1 (
  echo [ERROR] Failed to install build dependencies.
  exit /b 1
)

if exist "%BUILD_DIR%" (
  echo [INFO] Cleaning temporary build folder...
  rmdir /s /q "%BUILD_DIR%"
)

if exist "%SPEC_DIR%" (
  echo [INFO] Cleaning temporary spec folder...
  rmdir /s /q "%SPEC_DIR%"
)
mkdir "%SPEC_DIR%"

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyyMMdd_HHmmss\")"') do set "BUILD_STAMP=%%i"
set "DIST_DIR=%DIST_ROOT%\%BUILD_STAMP%"
mkdir "%DIST_DIR%"

echo [INFO] Build timestamp: %BUILD_STAMP%
echo [INFO] Dist output for this build: %DIST_DIR%

echo [INFO] Running PyInstaller (onedir)...
python -m PyInstaller ^
  --noconfirm ^
  --onedir ^
  --windowed ^
  --name "%APP_NAME%" ^
  --add-data "%TOOLKIT_DIR%;dcm4che-5.34.2" ^
  --distpath "%DIST_DIR%" ^
  --workpath "%BUILD_DIR%" ^
  --specpath "%SPEC_DIR%" ^
  "%APP_FILE%"
if errorlevel 1 (
  echo [ERROR] PyInstaller build failed.
  exit /b 1
)

echo.
echo [INFO] Build completed successfully.
echo [INFO] Output folder:
echo        %DIST_DIR%\%APP_NAME%
echo.
echo [INFO] Client run command:
echo        "%DIST_DIR%\%APP_NAME%\%APP_NAME%.exe"
echo.

endlocal
exit /b 0
