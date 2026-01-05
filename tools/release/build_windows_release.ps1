param(
    [Parameter(Mandatory = $true)]
    [string]$Version
)

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

# Arch tag (prefer OS bitness)
$archTag = "unknown"
if ($osInfo.OSArchitecture) {
    # e.g., "64-bit"
    if ($osInfo.OSArchitecture -match "64") { $archTag = "x86_64" }
    elseif ($osInfo.OSArchitecture -match "32") { $archTag = "x86" }
} else {
    # fallback
    $archTag = if ([Environment]::Is64BitOperatingSystem) { "x86_64" } else { "x86" }
}

# 1) Build the release binary
cargo build --release

# 2) Variables for packaging
$version = $Version
$appName = "flowlog"
$targetDir = "${appName}-${version}-${osTag}-${archTag}"
$targetPath = Join-Path -Path (Get-Location) -ChildPath $targetDir

# 3) Create a clean release directory
if (Test-Path $targetPath) {
    Remove-Item $targetPath -Recurse -Force
}
New-Item -ItemType Directory -Path $targetPath | Out-Null

# 4) Copy the binary
$sourceBinary = Join-Path -Path (Get-Location) -ChildPath "target/release/$appName.exe"
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

# 6) Remove the staging directory, keep only the archive
Remove-Item $targetPath -Recurse -Force

Write-Host "Release artifact created: $zipName"

# Running command:
# powershell -ExecutionPolicy Bypass -File .\tools\release\build_windows_release.ps1 -Version <version>
