# Marathon Intel — Network Capture Agent Installer
# Run in PowerShell as Administrator:
#   irm https://marathon.straightfirefood.blog/static/install-capture.ps1 | iex

$ErrorActionPreference = "Stop"
$API_URL = "https://marathon.straightfirefood.blog"
$AGENT_URL = "$API_URL/static/netcapture.py"
$INSTALL_DIR = "$env:USERPROFILE\MarathonIntel"

Write-Host ""
Write-Host "  ======================================" -ForegroundColor Green
Write-Host "   MARATHON INTEL - Capture Agent Setup" -ForegroundColor Green
Write-Host "  ======================================" -ForegroundColor Green
Write-Host ""

# Check admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "[!] This script must be run as Administrator." -ForegroundColor Red
    Write-Host "    Right-click PowerShell -> Run as Administrator" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
Write-Host "[+] Running as Administrator" -ForegroundColor Green

# Check/install Python
$python = $null
foreach ($cmd in @("python", "python3", "py")) {
    try {
        $ver = & $cmd --version 2>&1
        if ($ver -match "Python 3\.(\d+)") {
            $minor = [int]$Matches[1]
            if ($minor -ge 10) {
                $python = $cmd
                Write-Host "[+] Found $ver" -ForegroundColor Green
                break
            }
        }
    } catch {}
}

if (-not $python) {
    Write-Host "[!] Python 3.10+ not found." -ForegroundColor Red
    Write-Host "    Install from: https://www.python.org/downloads/" -ForegroundColor Yellow
    Write-Host "    Make sure to check 'Add Python to PATH' during install." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

# Check/install tshark
$tshark = $null
$wiresharkPaths = @(
    "C:\Program Files\Wireshark",
    "C:\Program Files (x86)\Wireshark",
    "$env:ProgramFiles\Wireshark"
)

# Check PATH first
try {
    $null = & tshark --version 2>&1
    $tshark = "tshark"
    Write-Host "[+] Found tshark in PATH" -ForegroundColor Green
} catch {
    # Check common install locations
    foreach ($dir in $wiresharkPaths) {
        $path = Join-Path $dir "tshark.exe"
        if (Test-Path $path) {
            $tshark = $path
            Write-Host "[+] Found tshark at $dir" -ForegroundColor Green
            break
        }
    }
}

if (-not $tshark) {
    Write-Host "[*] tshark not found. Attempting install via winget..." -ForegroundColor Yellow
    try {
        winget install --id WiresharkFoundation.Wireshark --accept-package-agreements --accept-source-agreements 2>&1 | Out-Null
        # Check again
        foreach ($dir in $wiresharkPaths) {
            $path = Join-Path $dir "tshark.exe"
            if (Test-Path $path) {
                $tshark = $path
                Write-Host "[+] Wireshark installed successfully" -ForegroundColor Green
                break
            }
        }
    } catch {
        Write-Host "[!] Could not auto-install Wireshark." -ForegroundColor Red
    }

    if (-not $tshark) {
        Write-Host "[!] Please install Wireshark manually from: https://www.wireshark.org/download.html" -ForegroundColor Red
        Write-Host "    Make sure 'TShark' is checked during installation." -ForegroundColor Yellow
        Write-Host ""
        exit 1
    }
}

# Create install directory
if (-not (Test-Path $INSTALL_DIR)) {
    New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
}
Write-Host "[+] Install directory: $INSTALL_DIR" -ForegroundColor Green

# Download the agent
Write-Host "[*] Downloading capture agent..." -ForegroundColor Yellow
$agentPath = Join-Path $INSTALL_DIR "netcapture.py"
try {
    Invoke-WebRequest -Uri $AGENT_URL -OutFile $agentPath -UseBasicParsing
    Write-Host "[+] Agent downloaded" -ForegroundColor Green
} catch {
    Write-Host "[!] Download failed: $_" -ForegroundColor Red
    exit 1
}

# Get username
Write-Host ""
$userHash = Read-Host "Enter your username/hash (used to track your data anonymously)"
if ([string]::IsNullOrWhiteSpace($userHash)) {
    $userHash = "anon_" + (Get-Random -Maximum 99999)
    Write-Host "[*] Using random hash: $userHash" -ForegroundColor Yellow
}

# Create a shortcut launcher
$launcherPath = Join-Path $INSTALL_DIR "Start-Capture.bat"
$launcherContent = @"
@echo off
title Marathon Intel - Capture Agent
echo.
echo   Marathon Intel - Network Capture Agent
echo   ======================================
echo   Username: $userHash
echo   Press Ctrl+C to stop
echo.
$python "$agentPath" --user-hash "$userHash" --api-url "$API_URL"
pause
"@
Set-Content -Path $launcherPath -Value $launcherContent

Write-Host ""
Write-Host "  ======================================" -ForegroundColor Green
Write-Host "   SETUP COMPLETE" -ForegroundColor Green
Write-Host "  ======================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Files installed to: $INSTALL_DIR" -ForegroundColor Cyan
Write-Host "  Your hash: $userHash" -ForegroundColor Cyan
Write-Host ""
Write-Host "  To run the agent:" -ForegroundColor White
Write-Host "    1. Open $launcherPath as Admin" -ForegroundColor White
Write-Host "    2. Play Marathon normally" -ForegroundColor White
Write-Host "    3. Data captures automatically" -ForegroundColor White
Write-Host ""

# Ask to run now
$runNow = Read-Host "Start capturing now? (y/n)"
if ($runNow -eq "y" -or $runNow -eq "Y") {
    Write-Host ""
    Write-Host "[*] Starting capture... Press Ctrl+C to stop." -ForegroundColor Green
    Write-Host ""
    & $python $agentPath --user-hash $userHash --api-url $API_URL
}
