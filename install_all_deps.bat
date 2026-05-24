@echo off
REM Install all project dependencies into the current Python (no virtualenv required).
setlocal EnableExtensions
cd /d "%~dp0"

set "PY_EXE=py"
where py >nul 2>&1 || set "PY_EXE=python"
"%PY_EXE%" -c "import sys" 2>nul || (
    echo [ERROR] Python not found. Install Python 3 and ensure py or python is on PATH.
    pause
    exit /b 1
)

echo Using: 
"%PY_EXE%" -c "import sys; print(sys.executable)"
echo.
"%PY_EXE%" -m pip install -U pip
"%PY_EXE%" -m pip install -r requirements.txt -r requirements-daemon.txt
if errorlevel 1 (
    echo.
    echo pip install failed.
    pause
    exit /b 1
)
echo.
echo Done.
pause
endlocal
