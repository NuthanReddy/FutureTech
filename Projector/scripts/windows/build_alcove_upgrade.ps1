<#
.SYNOPSIS
    Compiles, patches, zipaligns, signs, and optionally deploys the 3-phase
    Turbo Alcove alignment & sidewall-avoidance upgrade (com.hysd.screen-turbo-alcove.apk).
#>
param(
    [string]$ProjectorIp = "",
    [switch]$Deploy,
    [string]$SdkRoot = "$env:LOCALAPPDATA\Android\Sdk",
    [string]$JavaHome = "C:\Program Files\Android\Android Studio\jbr"
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$ProjectorDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$AlcoveDir    = Join-Path $ProjectorDir "alcove_upgrade"
$StockApk     = Join-Path $ProjectorDir "projector_artifacts\com.hysd.screen.apk"
$WorkDir      = Join-Path $env:TEMP ("alcove_build_" + [Guid]::NewGuid().ToString("N").Substring(0, 8))

if (-not (Test-Path -LiteralPath $JavaHome)) {
    if ($env:JAVA_HOME -and (Test-Path -LiteralPath $env:JAVA_HOME)) {
        $JavaHome = $env:JAVA_HOME
    } else {
        throw "JDK not found at '$JavaHome'. Pass -JavaHome or set JAVA_HOME."
    }
}

$btDir   = (Get-ChildItem "$SdkRoot\build-tools" | Sort-Object Name -Descending | Select-Object -First 1).FullName
$platJar = (Get-ChildItem "$SdkRoot\platforms\*\android.jar" | Sort-Object FullName -Descending | Select-Object -First 1).FullName
$env:JAVA_HOME = $JavaHome
$env:PATH      = "$JavaHome\bin;$env:PATH"

New-Item -ItemType Directory -Force -Path "$WorkDir\stubs_classes", "$WorkDir\app_classes", "$WorkDir\dex_out" | Out-Null

try {
    Write-Host "1. Compiling framework & OEM stubs ..."
    $stubSrcs = Get-ChildItem "$AlcoveDir\stubs" -Recurse -Filter *.java | ForEach-Object { $_.FullName }
    & "$JavaHome\bin\javac.exe" --release 8 -cp $platJar -d "$WorkDir\stubs_classes" $stubSrcs
    if ($LASTEXITCODE -ne 0) { throw "javac failed on stubs" }
    & "$JavaHome\bin\jar.exe" cf "$WorkDir\stubs.jar" -C "$WorkDir\stubs_classes" .

    Write-Host "2. Compiling upgraded VAF2S + AlcoveVisionPipeline ..."
    $appSrcs = Get-ChildItem "$AlcoveDir\src" -Recurse -Filter *.java | ForEach-Object { $_.FullName }
    & "$JavaHome\bin\javac.exe" --release 8 -cp "$WorkDir\stubs.jar;$platJar" -d "$WorkDir\app_classes" $appSrcs
    if ($LASTEXITCODE -ne 0) { throw "javac failed on app sources" }
    & "$JavaHome\bin\jar.exe" cf "$WorkDir\alcove_engine.jar" -C "$WorkDir\app_classes" .

    Write-Host "3. Dexing alcove_engine.jar -> classes3.dex ..."
    & "$btDir\d8.bat" --min-api 26 --lib $platJar --classpath "$WorkDir\stubs.jar" --output "$WorkDir\dex_out" "$WorkDir\alcove_engine.jar"
    if ($LASTEXITCODE -ne 0) { throw "d8 failed" }

    Write-Host "4. Binary-patching stock classes.dex (Lcom/hysd/hyscreen/VAF2S; -> VAF2T) + recomputing SHA-1 & Adler32 ..."
    Copy-Item -LiteralPath $StockApk -Destination "$WorkDir\unsigned.apk" -Force
    $za = [System.IO.Compression.ZipFile]::Open("$WorkDir\unsigned.apk", [System.IO.Compression.ZipArchiveMode]::Update)
    try {
        $dexEntry = $za.GetEntry("classes.dex")
        $ms = New-Object System.IO.MemoryStream
        $es = $dexEntry.Open()
        $es.CopyTo($ms)
        $es.Close()
        $dexBytes = $ms.ToArray()

        $oldPat = [System.Text.Encoding]::ASCII.GetBytes("Lcom/hysd/hyscreen/VAF2S;")
        $newPat = [System.Text.Encoding]::ASCII.GetBytes("Lcom/hysd/hyscreen/VAF2T;")
        $patched = 0
        for ($i = 0; $i -le $dexBytes.Length - $oldPat.Length; $i++) {
            if ($dexBytes[$i] -eq $oldPat[0] -and $dexBytes[$i + 23] -eq $oldPat[23]) {
                $match = $true
                for ($j = 0; $j -lt $oldPat.Length; $j++) {
                    if ($dexBytes[$i + $j] -ne $oldPat[$j]) { $match = $false; break }
                }
                if ($match) {
                    [Array]::Copy($newPat, 0, $dexBytes, $i, $newPat.Length)
                    $patched++
                    $i += $oldPat.Length - 1
                }
            }
        }
        if ($patched -eq 0) { throw "Descriptor Lcom/hysd/hyscreen/VAF2S; not found in stock classes.dex" }

        # Recompute DEX SHA-1 signature (bytes 12..31 over bytes 32..end)
        $sha1 = [System.Security.Cryptography.SHA1]::Create()
        $sig  = $sha1.ComputeHash($dexBytes, 32, $dexBytes.Length - 32)
        [Array]::Copy($sig, 0, $dexBytes, 12, 20)

        # Recompute DEX Adler32 checksum (bytes 8..11 little-endian over bytes 12..end)
        [uint32]$s1 = 1; [uint32]$s2 = 0
        $idx = 12
        while ($idx -lt $dexBytes.Length) {
            $end = [Math]::Min($idx + 5552, $dexBytes.Length)
            while ($idx -lt $end) {
                $s1 += $dexBytes[$idx]
                $s2 += $s1
                $idx++
            }
            $s1 %= 65521
            $s2 %= 65521
        }
        [uint32]$adler = ($s2 -shl 16) -bor $s1
        $adlerBytes = [BitConverter]::GetBytes($adler)
        [Array]::Copy($adlerBytes, 0, $dexBytes, 8, 4)

        $dexEntry.Delete()
        $oldC3 = $za.GetEntry("classes3.dex")
        if ($oldC3) { $oldC3.Delete() }

        $newDex = $za.CreateEntry("classes.dex", [System.IO.Compression.CompressionLevel]::NoCompression)
        $ns = $newDex.Open(); $ns.Write($dexBytes, 0, $dexBytes.Length); $ns.Close()

        $c3Bytes = [System.IO.File]::ReadAllBytes("$WorkDir\dex_out\classes.dex")
        $newC3 = $za.CreateEntry("classes3.dex", [System.IO.Compression.CompressionLevel]::Optimal)
        $cs = $newC3.Open(); $cs.Write($c3Bytes, 0, $c3Bytes.Length); $cs.Close()
        Write-Host "   Patched $patched occurrences in classes.dex."
    } finally {
        $za.Dispose()
    }

    Write-Host "5. Ensuring public AOSP platform testkey (fetched on demand into gitignored keys/) ..."
    $KeysDir = Join-Path $AlcoveDir "keys"
    New-Item -ItemType Directory -Force -Path $KeysDir | Out-Null
    $aospSecUrl = "https://android.googlesource.com/platform/build/+/refs/heads/main/target/product/security"
    foreach ($kf in @("platform.pk8", "platform.x509.pem")) {
        $kp = Join-Path $KeysDir $kf
        if (-not (Test-Path -LiteralPath $kp)) {
            Write-Host "   Downloading $kf from AOSP Gitiles ..."
            $resp = Invoke-WebRequest -Uri "$aospSecUrl/$kf`?format=TEXT" -UseBasicParsing
            $b64  = if ($resp.Content -is [byte[]]) { [System.Text.Encoding]::ASCII.GetString($resp.Content) } else { [string]$resp.Content }
            [System.IO.File]::WriteAllBytes($kp, [Convert]::FromBase64String($b64))
        }
    }
    $certDer = (New-Object System.Security.Cryptography.X509Certificates.X509Certificate2("$KeysDir\platform.x509.pem")).RawData
    $certSha = ([BitConverter]::ToString([System.Security.Cryptography.SHA256]::Create().ComputeHash($certDer)) -replace '-','').ToLower()
    if ($certSha -ne "c8a2e9bccf597c2fb6dc66bee293fc13f2fc47ec77bc6b2b0d52c11f51192ab8") {
        throw "Unexpected AOSP platform.x509.pem SHA-256: $certSha"
    }

    Write-Host "6. Zipaligning and signing with verified AOSP platform key ($certSha) ..."
    $outApk = Join-Path $WorkDir "com.hysd.screen-turbo-alcove.apk"
    & "$btDir\zipalign.exe" -p -f 4 "$WorkDir\unsigned.apk" "$WorkDir\aligned.apk"
    & "$btDir\apksigner.bat" sign `
        --key "$KeysDir\platform.pk8" `
        --cert "$KeysDir\platform.x509.pem" `
        --out $outApk `
        "$WorkDir\aligned.apk"
    if ($LASTEXITCODE -ne 0) { throw "apksigner failed" }

    $destApk = Join-Path $AlcoveDir "build\com.hysd.screen-turbo-alcove.apk"
    Copy-Item -LiteralPath $outApk -Destination $destApk -Force
    Write-Host "Built signed APK: $destApk"

    if ($Deploy) {
        if ([string]::IsNullOrWhiteSpace($ProjectorIp)) {
            throw "Specify -ProjectorIp when passing -Deploy."
        }
        $adb = "$SdkRoot\platform-tools\adb.exe"
        & $adb connect "${ProjectorIp}:5555" | Out-Host
        & $adb -s "${ProjectorIp}:5555" install -r -d $destApk
        & $adb -s "${ProjectorIp}:5555" shell "cmd package compile -m speed -f com.hysd.hyscreen"
        Write-Host "Deployed and AOT-compiled on ${ProjectorIp}:5555!"
    }
} finally {
    Remove-Item -LiteralPath $WorkDir -Recurse -Force -ErrorAction SilentlyContinue
}
