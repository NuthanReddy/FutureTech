<#
.SYNOPSIS
    Crops a photo to 16:9 (1920x1080), generates both the 6,220,854-byte 24-bit BGR
    BOOTLOGO.BMP and STORE-only /Reserve0/bootanimation.zip, flashes them safely
    preserving trailing FAT16 cluster bytes, and verifies SHA-256 on the projector.
#>
param(
    [string]$PhotoPath = "C:\Path\To\Photo.jpg",
    [int]$CropTopY = 850,
    [string]$ProjectorIp = "",
    [string]$ProjectorMac = "3C-BE-8E-43-78-87",
    [string]$AdbPath = "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe"
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing
Add-Type -AssemblyName System.IO.Compression

if (-not (Test-Path -LiteralPath $PhotoPath)) {
    throw "Photo not found at '$PhotoPath'. Pass -PhotoPath 'C:\Path\To\Your\Photo.jpg'."
}

if ([string]::IsNullOrWhiteSpace($ProjectorIp)) {
    $neighbor = Get-NetNeighbor -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.LinkLayerAddress -replace ':','-' -ieq ($ProjectorMac -replace ':','-') } |
        Select-Object -First 1
    if (-not $neighbor) { throw "Pass -ProjectorIp explicitly (MAC $ProjectorMac not in ARP cache)." }
    $ProjectorIp = $neighbor.IPAddress
}

$outDir = Join-Path $env:TEMP "projector_boot"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

# 1. Crop to exact 16:9 and resize to 1920x1080 24-bit RGB
$src   = [System.Drawing.Bitmap]::FromFile($PhotoPath)
$cropW = $src.Width
$cropH = [int][Math]::Round($cropW * 9.0 / 16.0)
if ($CropTopY + $cropH -gt $src.Height) { $CropTopY = [Math]::Max(0, $src.Height - $cropH) }

$dst = New-Object System.Drawing.Bitmap(1920, 1080, [System.Drawing.Imaging.PixelFormat]::Format24bppRgb)
$g   = [System.Drawing.Graphics]::FromImage($dst)
$g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
$g.DrawImage(
    $src,
    (New-Object System.Drawing.Rectangle(0, 0, 1920, 1080)),
    (New-Object System.Drawing.Rectangle(0, $CropTopY, $cropW, $cropH)),
    [System.Drawing.GraphicsUnit]::Pixel
)
$g.Dispose(); $src.Dispose()

# 2. Save 1920x1080 JPEG and pack STORE-only bootanimation.zip
$jpgCodec  = [System.Drawing.Imaging.ImageCodecInfo]::GetImageEncoders() | Where-Object { $_.MimeType -eq "image/jpeg" }
$encParams = New-Object System.Drawing.Imaging.EncoderParameters(1)
$encParams.Param[0] = New-Object System.Drawing.Imaging.EncoderParameter([System.Drawing.Imaging.Encoder]::Quality, 95L)
$dst.Save("$outDir\01.jpg", $jpgCodec, $encParams)

Remove-Item "$outDir\bootanimation.zip" -Force -ErrorAction SilentlyContinue
$fs = [System.IO.File]::Open("$outDir\bootanimation.zip", [System.IO.FileMode]::Create)
$za = New-Object System.IO.Compression.ZipArchive($fs, [System.IO.Compression.ZipArchiveMode]::Create)
$dEntry = $za.CreateEntry("desc.txt", [System.IO.Compression.CompressionLevel]::NoCompression)
$ds = $dEntry.Open(); $db = [System.Text.Encoding]::ASCII.GetBytes("1920 1080 15`np 0 0 part0`n"); $ds.Write($db, 0, $db.Length); $ds.Close()
$iEntry = $za.CreateEntry("part0/01.jpg", [System.IO.Compression.CompressionLevel]::NoCompression)
$is = $iEntry.Open(); $ib = [System.IO.File]::ReadAllBytes("$outDir\01.jpg"); $is.Write($ib, 0, $ib.Length); $is.Close()
$za.Dispose(); $fs.Close()

# 3. Build exact 6,220,854-byte uncompressed 24-bit bottom-up BGR BOOTLOGO.BMP
$bmpBytes = New-Object byte[] 6220854
$hdrHex   = "424d36ec5e000000000036000000280000008007000038040000010018000000000000ec5e00c40e0000c40e00000000000000000000"
for ($i = 0; $i -lt 54; $i++) { $bmpBytes[$i] = [Convert]::ToByte($hdrHex.Substring($i * 2, 2), 16) }

$lock = $dst.LockBits(
    (New-Object System.Drawing.Rectangle(0, 0, 1920, 1080)),
    [System.Drawing.Imaging.ImageLockMode]::ReadOnly,
    [System.Drawing.Imaging.PixelFormat]::Format24bppRgb
)
$row = New-Object byte[] 5760
for ($y = 0; $y -lt 1080; $y++) {
    [System.Runtime.InteropServices.Marshal]::Copy([IntPtr]::Add($lock.Scan0, (1079 - $y) * $lock.Stride), $row, 0, 5760)
    [Array]::Copy($row, 0, $bmpBytes, 54 + $y * 5760, 5760)
}
$dst.UnlockBits($lock); $dst.Dispose()
[System.IO.File]::WriteAllBytes("$outDir\custom_bootlogo.bmp", $bmpBytes)

# 4. Push, splice preserving 458 trailing bytes of LBA 138983, flash, and verify SHA-256
$target = "${ProjectorIp}:5555"
$bmpSha = (Get-FileHash "$outDir\custom_bootlogo.bmp" -Algorithm SHA256).Hash.ToLower()
& $AdbPath connect $target | Out-Host
& $AdbPath -s $target push "$outDir\bootanimation.zip" "/data/local/tmp/bootanimation.zip"
& $AdbPath -s $target push "$outDir\custom_bootlogo.bmp" "/data/local/tmp/custom_bootlogo.bmp"

& $AdbPath -s $target shell '
cat /data/local/tmp/bootanimation.zip > /Reserve0/bootanimation.zip &&
dd if=/dev/block/mmcblk0 of=/data/local/tmp/bl_tail.bin bs=512 skip=138983 count=1 2>/dev/null &&
cat /data/local/tmp/custom_bootlogo.bmp > /data/local/tmp/custom_bootlogo_padded.bmp &&
dd if=/data/local/tmp/bl_tail.bin bs=1 skip=54 count=458 2>/dev/null >> /data/local/tmp/custom_bootlogo_padded.bmp &&
dd if=/data/local/tmp/custom_bootlogo_padded.bmp of=/dev/block/mmcblk0 bs=512 seek=126833 count=12151 conv=notrunc,fsync &&
sync &&
rm -f /data/local/tmp/bootanimation.zip /data/local/tmp/custom_bootlogo*.bmp /data/local/tmp/bl_tail.bin
'

$verifySha = ((& $AdbPath -s $target shell "dd if=/dev/block/mmcblk0 bs=512 skip=126833 count=12151 2>/dev/null | head -c 6220854 | sha256sum").Trim() -split '\s+')[0].ToLower()
if ($verifySha -ne $bmpSha) { throw "SHA-256 mismatch! Expected $bmpSha, got $verifySha" }
Write-Host "Boot splash + Android bootanimation flashed and SHA-256 verified ($verifySha)!"
