@echo off
cd /d "%~dp0"
echo ====================================================================
echo Pushing CDC Data Pipeline Project to GitHub
echo Repository: https://github.com/Malleswarikomara/CDC_DataPipeline-Project.git
echo ====================================================================
echo.
git push -u origin main
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ====================================================================
    echo SUCCESS! Project successfully pushed to GitHub.
    echo View at: https://github.com/Malleswarikomara/CDC_DataPipeline-Project
    echo ====================================================================
) else (
    echo.
    echo Standard git push encountered an issue.
    echo You can upload directly via GitHub Token:
    echo python scripts/push_to_github.py --token YOUR_TOKEN
)
echo.
pause
