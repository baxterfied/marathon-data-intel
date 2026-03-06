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

# Install scapy (lightweight packet capture — no Wireshark needed)
Write-Host "[*] Installing scapy (packet capture library)..." -ForegroundColor Yellow
$pipResult = Start-Process -FilePath $python -ArgumentList "-m pip install scapy" -NoNewWindow -Wait -PassThru
if ($pipResult.ExitCode -eq 0) {
    Write-Host "[+] scapy installed" -ForegroundColor Green
} else {
    # Check if already installed
    $checkResult = Start-Process -FilePath $python -ArgumentList "-c `"import scapy`"" -NoNewWindow -Wait -PassThru
    if ($checkResult.ExitCode -eq 0) {
        Write-Host "[+] scapy already installed" -ForegroundColor Green
    } else {
        Write-Host "[!] Could not install scapy. Try manually: pip install scapy" -ForegroundColor Red
    }
}

# Windows needs Npcap for raw packet capture
$hasNpcap = (Test-Path "C:\Windows\System32\Npcap") -or (Test-Path "C:\Program Files\Npcap")
$hasPcap = (Test-Path "C:\Windows\System32\wpcap.dll") -or (Test-Path "C:\Windows\SysWOW64\wpcap.dll")
if ((-not $hasNpcap) -and (-not $hasPcap)) {
    Write-Host "[!] Npcap not detected. scapy needs Npcap for packet capture on Windows." -ForegroundColor Yellow
    Write-Host "    Download from: https://npcap.com/#download" -ForegroundColor Yellow
    Write-Host "    Install with 'WinPcap API-compatible Mode' checked." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "    If you already have Wireshark installed, Npcap is included." -ForegroundColor Cyan
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
$userHash = Read-Host "Pick a gamertag (e.g. xNightRunner — used to track your stats)"
if ([string]::IsNullOrWhiteSpace($userHash)) {
    $userHash = "anon_" + (Get-Random -Maximum 99999)
    Write-Host "[*] Using random gamertag: $userHash" -ForegroundColor Yellow
}

# Create a shortcut launcher
$launcherPath = Join-Path $INSTALL_DIR "Start-Capture.bat"
$launcherContent = @"
@echo off
:: Auto-elevate to Administrator
net session >nul 2>&1
if %errorlevel% neq 0 (
    powershell -Command "Start-Process cmd -ArgumentList '/c \"%~f0\"' -Verb RunAs"
    exit /b
)
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
Write-Host "  Your gamertag: $userHash" -ForegroundColor Cyan
Write-Host ""
Write-Host "  To run the agent:" -ForegroundColor White
Write-Host "    1. Double-click $launcherPath" -ForegroundColor White
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
