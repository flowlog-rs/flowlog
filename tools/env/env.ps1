# tools/env/env.ps1 — one-time machine setup for the FlowLog correctness
# stack on Windows.
#
# Run this **once** in an elevated PowerShell on a fresh dev box / runner
# image. It:
#   1. Installs rustup + a stable toolchain (if missing).
#   2. Installs the helper packages tests need via Chocolatey.
#   3. Runs `cargo check --workspace` as a smoke test of the install.
#
# It is intentionally not idempotent in the *minimal* sense: re-running
# is safe, but it will check/refresh package versions. It is **not** the
# right thing to call on every test run — that's `make doctor` (read-only
# health probe) and the per-suite scripts.
#
# This script targets the **flowlog** repo only (correctness). Perf-side
# deps (souffle, duckdb, GNU time) live with the sibling `flowlog-bench`
# repo and its own env.ps1 — see ../../AGENTS.md for the split.
#
# Exit codes:
#   0  bootstrap completed; `cargo check --workspace` succeeded
#   1  bootstrap failed (missing dependency, network failure, build break)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$ScriptName = Split-Path $PSCommandPath -Leaf

function Log     { param($m) Write-Host "[$ScriptName] $m" -ForegroundColor Cyan }
function LogWarn { param($m) Write-Warning "[$ScriptName] $m" }
function LogDie  { param($m) Write-Error "[$ScriptName] $m"; exit 1 }

# Require an elevated session so Chocolatey/package installs succeed.
$windowsPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $windowsPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    LogDie "Please run this script in an elevated PowerShell session (Run as Administrator)."
}

$osInfo = Get-CimInstance Win32_OperatingSystem
Log ("bootstrapping FlowLog correctness env on " + $osInfo.Caption + " (" + $osInfo.Version + ")")

# ----------------------------------------------------------------------
# 1. Chocolatey bootstrap
# ----------------------------------------------------------------------
function Install-Chocolatey {
    Log "installing Chocolatey..."
    Set-ExecutionPolicy Bypass -Scope Process -Force
    Invoke-Expression ((Invoke-WebRequest -UseBasicParsing -Uri 'https://community.chocolatey.org/install.ps1').Content)
}

if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    Install-Chocolatey
} else {
    Log "Chocolatey already installed."
}

# ----------------------------------------------------------------------
# 2. Chocolatey packages
# ----------------------------------------------------------------------
# Required for the correctness suites:
#   - protoc          : crates depend on prost-build
#   - python3         : diff math in fixture/oracle helpers
#   - wget, unzip     : oracle reference fetch + extract
#   - 7zip            : extracting tarballs (Windows ships tar since
#                       Win10 1803, but include 7zip for older boxes)
#   - git             : the bash-side helpers shell out to git
$ChocoPackages = @('protoc', 'python3', 'wget', 'unzip', '7zip', 'git')

Log "installing choco packages: $($ChocoPackages -join ', ')"
foreach ($pkg in $ChocoPackages) {
    choco install -y --no-progress $pkg
    if ($LASTEXITCODE -ne 0) { LogWarn "choco install $pkg returned $LASTEXITCODE" }
}

# ----------------------------------------------------------------------
# 3. rustup + stable toolchain
# ----------------------------------------------------------------------
function Install-Rustup {
    Log "installing rustup..."
    $installer = Join-Path $env:TEMP "rustup-init.exe"
    Invoke-WebRequest -UseBasicParsing -Uri "https://win.rustup.rs/x86_64" -OutFile $installer
    & $installer -y --default-toolchain stable --profile minimal
    Remove-Item $installer -Force
}

if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    Install-Rustup
    $env:PATH = "$env:USERPROFILE\.cargo\bin;" + $env:PATH
} else {
    Log "rustup already installed."
}

Log "ensuring stable toolchain is installed and default..."
rustup install stable
rustup default stable

# ----------------------------------------------------------------------
# 4. Smoke test
# ----------------------------------------------------------------------
Log "running 'cargo check --workspace' as smoke test..."
Push-Location $RootDir
try {
    cargo check --workspace
    if ($LASTEXITCODE -ne 0) { LogDie "cargo check --workspace failed" }
    Log "cargo check --workspace OK"
} finally {
    Pop-Location
}

Log "done. Next:  make doctor   then   make test"
