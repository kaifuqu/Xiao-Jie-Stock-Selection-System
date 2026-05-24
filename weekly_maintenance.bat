@echo off
setlocal EnableExtensions
cd /d "%~dp0"

REM Optional: run the same weekly orchestration as daemon 03:30 (scheme 2: stop holders then VACUUM).
REM Use when you prefer Task Scheduler instead of relying on auto_sniper_daemon to spawn the child.

set "PY_EXE=py"
where py >nul 2>&1 || set "PY_EXE=python"
"%PY_EXE%" -c "import sys" 2>nul || (
    echo [ERROR] Python not found.
    exit /b 1
)

"%PY_EXE%" "%~dp0tools\weekly_db_maintenance_orchestrated.py" --no-pause
set "RC=%ERRORLEVEL%"
exit /b %RC%
