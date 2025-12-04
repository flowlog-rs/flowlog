Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[SETUP] Setting up FlowLog development environment (Windows)..."

# Helper: ensure the script runs elevated so Chocolatey/package installs succeed
$windowsPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$runningAsAdmin = $windowsPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $runningAsAdmin) {
    Write-Warning "Please run this script in an elevated PowerShell session (Run as Administrator)."
    exit 1
}

# Detect Windows version for logging
$osInfo = Get-CimInstance Win32_OperatingSystem
$osCaption = $osInfo.Caption
$osVersion = $osInfo.Version
Write-Host "[DETECT] Operating system: $osCaption ($osVersion)"

# Chocolatey bootstrap (if missing)
function Install-Chocolatey {
    Write-Host "[INSTALL] Installing Chocolatey..."
    Set-ExecutionPolicy Bypass -Scope Process -Force
    Invoke-Expression ((Invoke-WebRequest -UseBasicParsing -Uri 'https://community.chocolatey.org/install.ps1').Content)
    Write-Host "[COMPLETE] Chocolatey installed."
}

if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    Install-Chocolatey
} else {
    Write-Host "[FOUND] Chocolatey already installed."
}

# Ensure required packages via Chocolatey
$packages = @("htop", "dos2unix")
if ($packages.Count -gt 0) {
    Write-Host "[INSTALL] Ensuring Windows packages: $($packages -join ', ')"
    foreach ($pkg in $packages) {
        choco install $pkg -y --no-progress | Out-Null
    }
}

# Ensure rustup
function Install-Rustup {
    Write-Host "[INSTALL] Installing rustup..."
    $installer = Join-Path $env:TEMP "rustup-init.exe"
    Invoke-WebRequest -UseBasicParsing -Uri "https://win.rustup.rs/x86_64" -OutFile $installer
    & $installer -y
    Remove-Item $installer -Force
    Write-Host "[COMPLETE] rustup installation finished."
}

if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    Install-Rustup
    $env:PATH = "$env:USERPROFILE\.cargo\bin;" + $env:PATH
} else {
    Write-Host "[FOUND] rustup already installed."
}

Write-Host "[UPDATE] Updating Rust toolchain to stable..."
rustup update stable
rustup default stable
Write-Host "[COMPLETE] Rust toolchain ready."

Write-Host "[VERIFY] Running cargo check..."
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $repoRoot
Push-Location $repoRoot
try {
    cargo check
    Write-Host "[COMPLETE] Environment setup completed successfully!"
} finally {
    Pop-Location
}

# Running command: powershell -ExecutionPolicy Bypass -File tools/env.ps1
