@echo off
setlocal enabledelayedexpansion

:: ============================================================================
::  Marathon Intel - Network Capture Agent Launcher (Windows)
:: ============================================================================

title Marathon Intel - Network Capture Agent
color 0B

echo.
echo  ============================================
echo   Marathon Intel - Network Capture Agent
echo  ============================================
echo.

:: ---------------------------------------------------------------------------
::  Auto-elevate to Administrator
:: ---------------------------------------------------------------------------

net session >nul 2>&1
if %errorlevel% neq 0 (
    echo  [*] Requesting Administrator privileges...
    powershell -Command "Start-Process cmd -ArgumentList '/c \"%~f0\" %*' -Verb RunAs"
    exit /b
)

color 0A
echo  [OK] Running with Administrator privileges.
color 0B

:: ---------------------------------------------------------------------------
::  Check for Python
:: ---------------------------------------------------------------------------

set "PYTHON="

where python >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON=python"
    goto :python_found
)

where python3 >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON=python3"
    goto :python_found
)

where py >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON=py -3"
    goto :python_found
)

color 0C
echo  [ERROR] Python is not installed or not in PATH.
echo.
echo  Install Python 3.10+:
echo.
echo    Download from: https://www.python.org/downloads/
echo    IMPORTANT: Check "Add Python to PATH" during installation.
echo.
pause
exit /b 1

:python_found
for /f "delims=" %%v in ('%PYTHON% --version 2^>^&1') do set PYVER=%%v
echo  [OK] Found %PYVER% (%PYTHON%)

:: ---------------------------------------------------------------------------
::  Check for capture backend (scapy or tshark)
:: ---------------------------------------------------------------------------

%PYTHON% -c "import scapy" >nul 2>&1
if %errorlevel% equ 0 (
    echo  [OK] Found scapy (capture backend)
    goto :backend_found
)

where tshark >nul 2>&1
if %errorlevel% equ 0 (
    echo  [OK] Found tshark (capture backend)
    goto :backend_found
)

:: Check default Wireshark install paths
set "WIRESHARK_PATHS="
set "WIRESHARK_PATHS=%WIRESHARK_PATHS%;C:\Program Files\Wireshark"
set "WIRESHARK_PATHS=%WIRESHARK_PATHS%;C:\Program Files (x86)\Wireshark"
set "WIRESHARK_PATHS=%WIRESHARK_PATHS%;%ProgramFiles%\Wireshark"

for %%p in (%WIRESHARK_PATHS%) do (
    if exist "%%~p\tshark.exe" (
        set "PATH=%%~p;!PATH!"
        echo  [OK] Found tshark at %%~p (capture backend)
        goto :backend_found
    )
)

color 0C
echo  [ERROR] No capture backend found.
echo.
echo  Install one of:
echo.
echo    Option 1 (recommended): pip install scapy
echo    Option 2: Install Wireshark from https://www.wireshark.org/download.html
echo.
pause
exit /b 1

:backend_found

:: ---------------------------------------------------------------------------
::  Check that netcapture.py exists
:: ---------------------------------------------------------------------------

set "SCRIPT_DIR=%~dp0"
set "CAPTURE_SCRIPT=%SCRIPT_DIR%netcapture.py"

if not exist "%CAPTURE_SCRIPT%" (
    color 0C
    echo  [ERROR] Cannot find netcapture.py at: %CAPTURE_SCRIPT%
    echo.
    pause
    exit /b 1
)

echo  [OK] Found capture script: %CAPTURE_SCRIPT%

:: ---------------------------------------------------------------------------
::  Get user hash (from argument or prompt)
:: ---------------------------------------------------------------------------

set "USER_HASH=%~1"

if "%USER_HASH%"=="" (
    echo.
    echo  Enter your username or hash (anonymous identifier for data correlation):
    set /p "USER_HASH=  > "
)

if "%USER_HASH%"=="" (
    color 0C
    echo  [ERROR] Username/hash cannot be empty.
    pause
    exit /b 1
)

echo  [OK] User hash: %USER_HASH%

:: ---------------------------------------------------------------------------
::  Set API URL
:: ---------------------------------------------------------------------------

set "API_URL=https://marathon.straightfirefood.blog"

:: ---------------------------------------------------------------------------
::  Launch the capture agent
:: ---------------------------------------------------------------------------

echo.
color 0B
echo  ============================================
echo   Starting Network Capture Agent
echo  ============================================
echo.
echo  API URL  : %API_URL%
echo  User     : %USER_HASH%
echo.
echo  Starting capture... Press Ctrl+C to stop.
echo.

%PYTHON% "%CAPTURE_SCRIPT%" --api-url "%API_URL%" --user-hash "%USER_HASH%"

if %errorlevel% neq 0 (
    echo.
    color 0C
    echo  [ERROR] Capture agent exited with an error (code %errorlevel%).
)

echo.
pause
exit /b %errorlevel%
