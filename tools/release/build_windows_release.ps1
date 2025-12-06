Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 0) Detect Windows version for packaging suffix
$osInfo = Get-CimInstance Win32_OperatingSystem
if (-not $osInfo) {
    Write-Error "Error: Unable to detect Windows version."
    exit 1
}

$osTag = "windows"
if ($osInfo.Version) {
    $versionParts = $osInfo.Version.Split('.')
    if ($versionParts.Length -ge 1) {
        $majorVersion = $versionParts[0]
        $osTag += $majorVersion
    } else {
        $osTag += $osInfo.Version.Replace('.', '')
    }
} else {
    $osTag += "unknown"
}

# 1) Build the release binary
cargo build --release

# 2) Variables for packaging
$version = "v0.1.0-internal"
$appName = "flowlog_compile"
$targetDir = "${appName}-${version}-${osTag}-x86_64"
$targetPath = Join-Path -Path (Get-Location) -ChildPath $targetDir

# 3) Create a clean release directory
if (Test-Path $targetPath) {
    Remove-Item $targetPath -Recurse -Force
}
New-Item -ItemType Directory -Path $targetPath | Out-Null

# 4) Copy and rename the binary: compiler.exe -> flowlog_compile.exe
$sourceBinary = Join-Path -Path (Get-Location) -ChildPath "target/release/compiler.exe"
if (-not (Test-Path $sourceBinary)) {
    Write-Error "Error: Expected binary '$sourceBinary' not found."
    exit 1
}
Copy-Item -Path $sourceBinary -Destination (Join-Path $targetPath "$appName.exe")

# Optionally include README / LICENSE
foreach ($doc in @("README.md", "LICENSE")) {
    if (Test-Path $doc) {
        Copy-Item $doc -Destination $targetPath
    }
}

# 5) Create the zip archive
$zipName = "${targetDir}.zip"
if (Test-Path $zipName) {
    Remove-Item $zipName -Force
}
Compress-Archive -Path $targetDir -DestinationPath $zipName

Write-Host "Release artifact created: $zipName"

# Running command: powershell -ExecutionPolicy Bypass -File .\tools\release\build_windows_release.ps1
