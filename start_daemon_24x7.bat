@echo off
REM start_daemon_24x7.bat - ASCII REM only for cmd.exe compatibility on Chinese Windows.
REM 7x24 daemon watchdog. No >> or > redirect to data\runtime logs - Python RotatingFileHandler needs exclusive rename.
setlocal EnableExtensions
cd /d "%~dp0"

chcp 65001 >nul 2>&1

if not exist "data\runtime" mkdir "data\runtime"

set "PY_EXE=py"
where py >nul 2>&1
if errorlevel 1 set "PY_EXE=python"

"%PY_EXE%" -c "import sys" 2>nul
if errorlevel 1 goto :NO_PY

echo [CHECK] verify_system.py - once before watchdog loop ...
"%PY_EXE%" "%~dp0verify_system.py"
if errorlevel 1 goto :VERIFY_FAIL

set "XIAOJIE_EMBED_UI_SCAN_WORKER=0"
set "PYTHONUNBUFFERED=1"

echo.
echo [WATCHDOG] Rotating log: data\runtime\sniper.log - see core\log_config.py
echo [WARN] One daemon only. Do not run start_server.bat or second copy on same DB.
echo [STOP] Press Ctrl+C to stop this window.
echo.

:LOOP
echo [START] auto_sniper_daemon.py
"%PY_EXE%" -u "%~dp0auto_sniper_daemon.py"
echo.
echo 守护进程已退出。为避免数据库维护/压缩期间被立即重拉起，看门狗将等待 60 秒后再自动重启...
timeout /t 60 /nobreak
goto LOOP

:NO_PY
echo [ERROR] Python not found. Install Python 3 and add py or python to PATH.
echo Then: py -m pip install -r requirements.txt -r requirements-daemon.txt
pause
exit /b 1

:VERIFY_FAIL
echo [ERROR] verify_system.py failed. Fix errors above, then run this bat again.
pause
exit /b 1
