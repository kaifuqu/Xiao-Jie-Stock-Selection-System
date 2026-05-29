@echo off
REM start_server.bat - ASCII REM only for cmd.exe compatibility on Chinese Windows.
REM Usage:
REM   start_server.bat       - 只启动 UI（推荐调试用）
REM   start_server.bat all   - 同时启动 daemon + UI
setlocal EnableExtensions
cd /d "%~dp0"

if not exist "data\runtime" mkdir "data\runtime"

set "PY_EXE=py"
where py >nul 2>&1 || set "PY_EXE=python"
"%PY_EXE%" -c "import sys" 2>nul || (
    echo [ERROR] Python not found. Install Python 3 and ensure py or python is on PATH.
    echo Then from project root: py -m pip install -r requirements.txt -r requirements-daemon.txt
    pause
    exit /b 1
)

REM Parse arguments
set "START_MODE=%~1"
if "%START_MODE%"=="" set "START_MODE=ui-only"
if "%START_MODE%"=="all" goto :START_ALL
if "%START_MODE%"=="ui-only" goto :START_UI_ONLY

:START_UI_ONLY
echo ================================================
echo  [模式] 仅启动 UI（无后台 daemon）
echo ================================================
echo  说明：此模式下 UI 直接操作数据库，适合调试。
echo        如果需要 7x24 自动扫描，请单独运行 start_daemon_24x7.bat
echo ================================================
echo.
set "XIAOJIE_EMBED_UI_SCAN_WORKER=1"
"%PY_EXE%" -m streamlit run "%~dp0ui\app.py"
if errorlevel 1 (
    echo.
    echo Streamlit exited with an error. See messages above.
    pause
)
goto :END

:START_ALL
echo ================================================
echo  [模式] 同时启动 daemon + UI
echo ================================================
echo  说明：daemon 后台运行做数据同步和自动扫描，UI 前台显示。
echo  注意：如果只想调试 UI，请用 start_server.bat（无参数）只启动 UI
echo ================================================
echo.

REM Daemon writes RotatingFileHandler to data\runtime\sniper.log
start "xiaojie-daemon" /B "%PY_EXE%" "%~dp0auto_sniper_daemon.py"

REM Stagger UI start
timeout /t 2 /nobreak >nul

set "XIAOJIE_EMBED_UI_SCAN_WORKER=0"
"%PY_EXE%" -m streamlit run "%~dp0ui\app.py"
if errorlevel 1 (
    echo.
    echo Streamlit exited with an error. See messages above.
    pause
)
goto :END

:END
endlocal
