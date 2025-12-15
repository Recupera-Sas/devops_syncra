@echo off
:: ============================================
:: Update the project and run the Python script
:: ============================================

:: 🧭 Move to the directory where this script is located
cd /d "%~dp0"

echo 🔄 Fetching latest changes from Git...
git fetch --all
git reset --hard origin/master
echo ✅ Repository successfully updated.

:: ============================================
:: Try running with soporteit virtual environment first
:: ============================================

set "SUPPORT_ENV=%USERPROFILE%\.virtualenvs\soporteit-hFdpDLPc\Scripts\python.exe"

echo 🚀 Running main.py...

if exist "%SUPPORT_ENV%" (
    echo 🐍 Trying soporteit environment...
    start "" /B "%SUPPORT_ENV%" main.py
    if %errorlevel% neq 0 (
        echo ⚠️ soporteit environment failed. Trying system Python...
        start "" /B python main.py
    )
) else (
    echo ⚠️ soporteit environment not found. Using system Python...
    start "" /B python main.py
)

exit