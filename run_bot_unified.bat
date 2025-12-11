@echo off
cd /d "%~dp0"

set "LOG_FILE=%~dp0run_bot_unified.log"

echo [START] %date% %time% >> "%LOG_FILE%"

if exist ".venv\Scripts\pythonw.exe" (
    start "" ".venv\Scripts\pythonw.exe" bot_unified.py
    echo [INFO] Started via pythonw.exe >> "%LOG_FILE%"
) else (
    start "" /min python bot_unified.py
    echo [INFO] Started via python >> "%LOG_FILE%"
)
