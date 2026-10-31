@echo off
cd /d "%~dp0"
echo ===================================================
echo Running 32 Pytest Automated Tests...
echo ===================================================
"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe" -m pytest -v
echo.
pause
