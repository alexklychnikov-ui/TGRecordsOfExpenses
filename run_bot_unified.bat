@echo off
setlocal

rem Change to project directory
cd /d "%~dp0"

rem Activate local venv if it exists
if exist ".venv\Scripts\activate.bat" (
    call ".venv\Scripts\activate.bat"
)

rem Prefer pythonw (no console); fallback to python
set "PY_CMD="
for %%P in (pythonw.exe python.exe) do (
    where %%P >nul 2>&1
    if not errorlevel 1 (
        set "PY_CMD=%%P"
        goto launch
    )
)
echo [ERROR] Python interpreter not found in PATH.
timeout /t 5 >nul
exit /b 1

:launch
start "" /min "%PY_CMD%" bot_unified.py
endlocal

