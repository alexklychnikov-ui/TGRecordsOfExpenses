@echo off
setlocal

echo [INFO] Attempting to stop all Python processes...
tasklist /FI "IMAGENAME eq python.exe" | find /I "python.exe" >nul && (
    echo [INFO] Killing python.exe
    taskkill /F /IM python.exe >nul 2>&1
) || (
    echo [INFO] python.exe not running
)

tasklist /FI "IMAGENAME eq pythonw.exe" | find /I "pythonw.exe" >nul && (
    echo [INFO] Killing pythonw.exe
    taskkill /F /IM pythonw.exe >nul 2>&1
) || (
    echo [INFO] pythonw.exe not running
)

echo [INFO] Done.
timeout /t 3 >nul
endlocal

