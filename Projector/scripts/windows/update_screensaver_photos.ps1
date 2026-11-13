<#
.SYNOPSIS
    Syncs a folder of 1920x1080 photos (.jpg/.jpeg/.png/.webp) to the projector's
    /sdcard/Screensaver and /Reserve0/screensaver directories and launches preview.
#>
param(
    [string]$PhotosDir = "C:\Path\To\Framed1080pPhotos",
    [string]$ProjectorIp = "",
    [string]$ProjectorMac = "3C-BE-8E-43-78-87",
    [string]$AdbPath = "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $PhotosDir)) {
    throw "Photos folder not found at '$PhotosDir'. Pass -PhotosDir 'C:\Path\To\Your\Photos'."
}

if ([string]::IsNullOrWhiteSpace($ProjectorIp)) {
    $neighbor = Get-NetNeighbor -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.LinkLayerAddress -replace ':','-' -ieq ($ProjectorMac -replace ':','-') } |
        Select-Object -First 1
    if (-not $neighbor) { throw "Pass -ProjectorIp explicitly (MAC $ProjectorMac not in ARP cache)." }
    $ProjectorIp = $neighbor.IPAddress
}

$images = Get-ChildItem -LiteralPath $PhotosDir -File |
    Where-Object { $_.Extension -in ".jpg", ".jpeg", ".png", ".webp" } |
    Sort-Object Name

if ($images.Count -eq 0) {
    throw "No .jpg/.jpeg/.png/.webp images found in '$PhotosDir'."
}

$target = "${ProjectorIp}:5555"
& $AdbPath connect $target | Out-Host
& $AdbPath -s $target shell "mkdir -p /sdcard/Screensaver /Reserve0/screensaver && rm -f /sdcard/Screensaver/* /Reserve0/screensaver/*"

foreach ($img in $images) {
    $safeName = $img.Name -replace '[^A-Za-z0-9._-]', '_'
    Write-Host "Uploading $safeName ..."
    & $AdbPath -s $target push $img.FullName "/data/local/tmp/$safeName"
    & $AdbPath -s $target shell "cat /data/local/tmp/$safeName > /sdcard/Screensaver/$safeName && cat /data/local/tmp/$safeName > /Reserve0/screensaver/$safeName && rm -f /data/local/tmp/$safeName"
}

& $AdbPath -s $target shell "sync && am start -n com.ngantla.personalscreensaver/.PersonalSlideshowActivity"
Write-Host "Synced $($images.Count) screensaver photo(s) and started live preview!"
