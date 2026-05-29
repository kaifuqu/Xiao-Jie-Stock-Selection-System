@echo off
REM start_server.bat - Launch UI with optional daemon
REM Usage:
REM   start_server.bat       - UI only (debug mode)
REM   start_server.bat all   - daemon + UI
setlocal EnableExtensions
cd /d "%~dp0"

if not exist "data\runtime" mkdir "data\runtime"

set "PY_EXE=py"
where py >nul 2>&1 || set "PY_EXE=python"
"%PY_EXE%" -c "import sys" 2>nul || (
    echo [ERROR] Python not found.
    pause
    exit /b 1
)

REM Parse arguments
set "START_MODE=%~1"
if "%START_MODE%"=="" goto :START_UI_ONLY
if "%START_MODE%"=="all" goto :START_ALL
echo Unknown argument: %START_MODE%
echo Usage: start_server.bat [all]
pause
exit /b 1

:START_UI_ONLY
echo ================================================
echo  [MODE] UI Only (no daemon)
echo ================================================
echo  This mode is for debugging. For 7x24 scanning,
echo  run start_daemon_24x7.bat separately.
echo ================================================
echo.
set "XIAOJIE_EMBED_UI_SCAN_WORKER=1"
"%PY_EXE%" -m streamlit run "%~dp0ui\app.py"
if errorlevel 1 (
    echo.
    echo Streamlit exited with error.
    pause
)
goto :END

:START_ALL
echo ================================================
echo  [MODE] Daemon + UI
echo ================================================
echo  Daemon runs in background for 7x24 scanning.
echo ================================================
echo.
start "xiaojie-daemon" /B "%PY_EXE%" "%~dp0auto_sniper_daemon.py"
timeout /t 2 /nobreak >nul
set "XIAOJIE_EMBED_UI_SCAN_WORKER=0"
"%PY_EXE%" -m streamlit run "%~dp0ui\app.py"
if errorlevel 1 (
    echo.
    echo Streamlit exited with error.
    pause
)
goto :END

:END
endlocal
