<#
.SYNOPSIS
    Connects to the WZATCO Yuva Blaze projector over Wi-Fi/Hotspot ADB and performs a
    SHA-256-verified backup of factory Tsai optical calibration and raw eMMC partitions.
#>
param(
    [string]$ProjectorIp = "",
    [string]$ProjectorMac = "3C-BE-8E-43-78-87",
    [string]$BackupDir = "$HOME\Backups\Wzatco-Yuva-Blaze",
    [string]$AdbPath = "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $AdbPath)) {
    $cmd = Get-Command adb -ErrorAction SilentlyContinue
    if ($cmd) { $AdbPath = $cmd.Source } else { throw "adb.exe not found at '$AdbPath' or in PATH." }
}

if ([string]::IsNullOrWhiteSpace($ProjectorIp)) {
    $neighbor = Get-NetNeighbor -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.LinkLayerAddress -replace ':','-' -ieq ($ProjectorMac -replace ':','-') } |
        Select-Object -First 1
    if (-not $neighbor) {
        throw "Could not auto-discover projector MAC $ProjectorMac in ARP table. Pass -ProjectorIp explicitly."
    }
    $ProjectorIp = $neighbor.IPAddress
}

$target = "${ProjectorIp}:5555"
Write-Host "Connecting to projector at $target ..."
& $AdbPath connect $target | Out-Host

New-Item -ItemType Directory -Force -Path "$BackupDir\calibration", "$BackupDir\partitions" | Out-Null

Write-Host "1. Backing up unit-unique factory Tsai stereo calibration (/sdcard/focus & /sdcard/focus_d) ..."
& $AdbPath -s $target pull /sdcard/focus "$BackupDir\calibration\focus"
& $AdbPath -s $target pull /sdcard/focus_d "$BackupDir\calibration\focus_d"

$partitions = @(
    @{ Name = "factorydata.img";  Skip = 73728;   Count = 32768 }, # 16 MiB
    @{ Name = "bootloader_a.img"; Skip = 106496;  Count = 65536 }, # 32 MiB (contains BOOTLOGO.BMP)
    @{ Name = "Reserve0_a.img";   Skip = 7422976; Count = 32768 }  # 16 MiB
)

foreach ($p in $partitions) {
    $outFile = Join-Path "$BackupDir\partitions" $p.Name
    Write-Host "2. Dumping $($p.Name) (LBA $($p.Skip), $($p.Count) sectors) ..."
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $AdbPath
    $psi.Arguments = "-s $target exec-out `"dd if=/dev/block/mmcblk0 bs=512 skip=$($p.Skip) count=$($p.Count) 2>/dev/null`""
    $psi.RedirectStandardOutput = $true
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    $fs = [System.IO.File]::Create($outFile)
    $proc.StandardOutput.BaseStream.CopyTo($fs)
    $fs.Close()
    $proc.WaitForExit()

    $pcSha  = (Get-FileHash -LiteralPath $outFile -Algorithm SHA256).Hash.ToLower()
    $devOut = (& $AdbPath -s $target shell "dd if=/dev/block/mmcblk0 bs=512 skip=$($p.Skip) count=$($p.Count) 2>/dev/null | sha256sum").Trim()
    $devSha = ($devOut -split '\s+')[0].ToLower()

    if ($pcSha -ne $devSha) {
        throw "SHA-256 mismatch for $($p.Name)! PC=$pcSha vs Device=$devSha"
    }
    Write-Host "   Verified $($p.Name) SHA-256: $pcSha"
}

Write-Host "Backup completed and verified in: $BackupDir"
