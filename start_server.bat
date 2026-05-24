@echo off
REM Keep this file ASCII-only: cmd.exe may misparse UTF-8 REM lines on Chinese Windows (garbled tokens -> 'not recognized').
setlocal EnableExtensions
cd /d "%~dp0"

REM Production: two processes -- daemon (background) + Streamlit UI (foreground).
REM 1) auto_sniper_daemon.py: sync, P1, scans, snapshots, async queue
REM 2) streamlit ui/app.py: web UI
REM Uses system Python (py launcher, or python on PATH). Install deps first:
REM   py -m pip install -r requirements.txt -r requirements-daemon.txt
REM   (or run install_all_deps.bat from project root)

if not exist "data\runtime" mkdir "data\runtime"

set "PY_EXE=py"
where py >nul 2>&1 || set "PY_EXE=python"
"%PY_EXE%" -c "import sys" 2>nul || (
    echo [ERROR] Python not found. Install Python 3 and ensure py or python is on PATH.
    echo Then from project root: py -m pip install -r requirements.txt -r requirements-daemon.txt
    pause
    exit /b 1
)

REM Daemon writes RotatingFileHandler to data\runtime\sniper.log; avoid shell redirect double-write.
start "xiaojie-daemon" /B "%PY_EXE%" "%~dp0auto_sniper_daemon.py"

REM Stagger UI start: daemon and Streamlit both touch quant_data.duckdb; same-moment open on Windows can log
REM "another program is using this file" before DuckDB settles (read_only retry usually succeeds).
timeout /t 2 /nobreak >nul

REM Force-disable UI embedded async queue worker in two-process deployment.
set "XIAOJIE_EMBED_UI_SCAN_WORKER=0"
"%PY_EXE%" -m streamlit run "%~dp0ui\app.py"
if errorlevel 1 (
    echo.
    echo Streamlit exited with an error. See messages above.
    pause
)

endlocal
