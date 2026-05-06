# env/env.ps1 — one-time machine setup for FlowLog development on
# Windows. Installs rustup + stable toolchain, helper packages via
# Chocolatey, then runs `cargo check --workspace` as a smoke test.
#
# Run once in an elevated PowerShell on a fresh dev box.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Log     { param($m) Write-Host "[env.ps1] $m" -ForegroundColor Cyan }
function LogWarn { param($m) Write-Warning "[env.ps1] $m" }
function LogDie  { param($m) Write-Error "[env.ps1] $m"; exit 1 }

# Chocolatey installs need an elevated session.
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    LogDie "run in an elevated PowerShell (Run as Administrator)"
}

$os = Get-CimInstance Win32_OperatingSystem
Log "bootstrapping FlowLog dev env on $($os.Caption) ($($os.Version))"

# Chocolatey
if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    Log "installing Chocolatey..."
    Set-ExecutionPolicy Bypass -Scope Process -Force
    Invoke-Expression ((Invoke-WebRequest -UseBasicParsing -Uri 'https://community.chocolatey.org/install.ps1').Content)
} else {
    Log "Chocolatey already installed"
}

# OS packages this repo needs:
#   protoc        : prost-build (build-time codegen)
#   python3       : Python helpers in this repo
#   wget, unzip   : fetch + extract dataset / reference archives
#   7zip          : tarball extraction (Win10 1803+ ships tar; 7zip covers older)
#   git           : bash-side helpers shell out to git
$ChocoPackages = @('protoc', 'python3', 'wget', 'unzip', '7zip', 'git')
Log "choco install: $($ChocoPackages -join ', ')"
foreach ($pkg in $ChocoPackages) {
    choco install -y --no-progress $pkg
    if ($LASTEXITCODE -ne 0) { LogWarn "choco install $pkg returned $LASTEXITCODE" }
}

# rustup + stable toolchain
if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    Log "installing rustup..."
    $installer = Join-Path $env:TEMP "rustup-init.exe"
    Invoke-WebRequest -UseBasicParsing -Uri "https://win.rustup.rs/x86_64" -OutFile $installer
    & $installer -y --default-toolchain stable --profile minimal
    Remove-Item $installer -Force
    # rustup-init writes to %USERPROFILE%\.cargo\bin but doesn't touch this session's PATH.
    $env:PATH = "$env:USERPROFILE\.cargo\bin;" + $env:PATH
} else {
    Log "rustup already installed"
}
rustup install stable
rustup default stable

Log "running cargo check --workspace..."
cargo check --workspace --manifest-path (Join-Path $RootDir 'Cargo.toml')
if ($LASTEXITCODE -ne 0) { LogDie "cargo check --workspace failed" }
Log "done."
