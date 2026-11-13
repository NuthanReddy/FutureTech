#!/usr/bin/env zsh
# Compiles, patches, zipaligns, signs, and optionally deploys com.hysd.screen-turbo-alcove.apk on macOS.
# Usage: ./build_alcove_upgrade.sh [PROJECTOR_IP]

set -euo pipefail

PROJ_IP="${1:-}"
SCRIPT_DIR="${0:A:h}"
REPO_PROJ="$(cd "$SCRIPT_DIR/../.." && pwd)"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
export ANDROID_SDK="${ANDROID_HOME:-$HOME/Library/Android/sdk}"
BT_DIR="$(ls -d "$ANDROID_SDK"/build-tools/* | sort -V | tail -n 1)"
PLAT_JAR="$(ls "$ANDROID_SDK"/platforms/*/android.jar | sort -V | tail -n 1)"
export PATH="$JAVA_HOME/bin:$BT_DIR:$PATH"

WORK_DIR="$(mktemp -d /tmp/alcove_build.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT
mkdir -p "$WORK_DIR/stubs_classes" "$WORK_DIR/app_classes" "$WORK_DIR/dex_out"

echo "1. Compiling framework & OEM stubs ..."
javac --release 8 -cp "$PLAT_JAR" \
  -d "$WORK_DIR/stubs_classes" \
  $(find "$REPO_PROJ/alcove_upgrade/stubs" -name '*.java')
jar cf "$WORK_DIR/stubs.jar" -C "$WORK_DIR/stubs_classes" .

echo "2. Compiling upgraded VAF2S + AlcoveVisionPipeline ..."
javac --release 8 -cp "$WORK_DIR/stubs.jar:$PLAT_JAR" \
  -d "$WORK_DIR/app_classes" \
  $(find "$REPO_PROJ/alcove_upgrade/src" -name '*.java')
jar cf "$WORK_DIR/alcove_engine.jar" -C "$WORK_DIR/app_classes" .

echo "3. Dexing bytecode -> classes3.dex ..."
"$BT_DIR/d8" --min-api 26 --lib "$PLAT_JAR" \
  --classpath "$WORK_DIR/stubs.jar" \
  --output "$WORK_DIR/dex_out" \
  "$WORK_DIR/alcove_engine.jar"

echo "4. Patching stock classes.dex (Lcom/hysd/hyscreen/VAF2S; -> VAF2T) with SHA-1 + Adler32 ..."
cp "$REPO_PROJ/projector_artifacts/com.hysd.screen.apk" "$WORK_DIR/unsigned.apk"
unzip -p "$WORK_DIR/unsigned.apk" classes.dex > "$WORK_DIR/classes.dex"

python3 - "$WORK_DIR/classes.dex" <<'PY'
import hashlib, struct, sys, zlib
p = sys.argv[1]
data = bytearray(open(p, "rb").read())
old, new = b"Lcom/hysd/hyscreen/VAF2S;", b"Lcom/hysd/hyscreen/VAF2T;"
cnt = data.count(old)
assert cnt > 0, "VAF2S descriptor not found"
data = data.replace(old, new)
data[12:32] = hashlib.sha1(data[32:]).digest()
struct.pack_into("<I", data, 8, zlib.adler32(data[12:]) & 0xFFFFFFFF)
open(p, "wb").write(data)
print(f"   Patched {cnt} occurrences in classes.dex")
PY

cp "$WORK_DIR/dex_out/classes.dex" "$WORK_DIR/classes3.dex"
zip -d "$WORK_DIR/unsigned.apk" classes.dex "classes3.dex" >/dev/null 2>&1 || true
(cd "$WORK_DIR" && zip -0 -X unsigned.apk classes.dex && zip -9 -X unsigned.apk classes3.dex)

echo "5. Zipaligning and signing with AOSP platform key ..."
OUT_APK="$REPO_PROJ/alcove_upgrade/build/com.hysd.screen-turbo-alcove.apk"
"$BT_DIR/zipalign" -p -f 4 "$WORK_DIR/unsigned.apk" "$WORK_DIR/aligned.apk"
"$BT_DIR/apksigner" sign \
  --key "$REPO_PROJ/alcove_upgrade/keys/platform.pk8" \
  --cert "$REPO_PROJ/alcove_upgrade/keys/platform.x509.pem" \
  --out "$OUT_APK" \
  "$WORK_DIR/aligned.apk"

echo "Built signed APK: $OUT_APK"

if [[ -n "$PROJ_IP" ]]; then
  adb connect "${PROJ_IP}:5555"
  adb -s "${PROJ_IP}:5555" install -r -d "$OUT_APK"
  adb -s "${PROJ_IP}:5555" shell "cmd package compile -m speed -f com.hysd.hyscreen"
  echo "Deployed and AOT-compiled on ${PROJ_IP}:5555!"
fi
