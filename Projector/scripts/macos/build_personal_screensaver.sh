#!/usr/bin/env zsh
# Builds PersonalScreenSaver.apk from Projector/personal_saver_app source on macOS
# and optionally deploys + activates PhotoDreamService & HtpSaverWatcherService.
# Usage: ./build_personal_screensaver.sh [PROJECTOR_IP]

set -euo pipefail

PROJ_IP="${1:-}"
SCRIPT_DIR="${0:A:h}"
REPO_PROJ="$(cd "$SCRIPT_DIR/../.." && pwd)"
SAVER_SRC="$REPO_PROJ/personal_saver_app"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
export ANDROID_SDK="${ANDROID_HOME:-$HOME/Library/Android/sdk}"
BT_DIR="$(ls -d "$ANDROID_SDK"/build-tools/* | sort -V | tail -n 1)"
PLAT_JAR="$(ls "$ANDROID_SDK"/platforms/*/android.jar | sort -V | tail -n 1)"
export PATH="$JAVA_HOME/bin:$BT_DIR:$PATH"

WORK_SAVER="$(mktemp -d /tmp/saver_build.XXXXXX)"
mkdir -p "$WORK_SAVER/gen" "$WORK_SAVER/classes" "$WORK_SAVER/dex"

"$BT_DIR/aapt2" link -o "$WORK_SAVER/res_unsigned.apk" \
  -I "$PLAT_JAR" \
  --manifest "$SAVER_SRC/AndroidManifest.xml" \
  --java "$WORK_SAVER/gen"

javac --release 8 -cp "$REPO_PROJ/alcove_upgrade/build/stubs.jar:$PLAT_JAR" \
  -d "$WORK_SAVER/classes" \
  $(find "$SAVER_SRC/src" "$WORK_SAVER/gen" -name '*.java')
jar cf "$WORK_SAVER/app.jar" -C "$WORK_SAVER/classes" .

"$BT_DIR/d8" --min-api 26 --lib "$PLAT_JAR" \
  --classpath "$REPO_PROJ/alcove_upgrade/build/stubs.jar" \
  --output "$WORK_SAVER/dex" \
  "$WORK_SAVER/app.jar"

KEYS_DIR="$REPO_PROJ/alcove_upgrade/keys"
mkdir -p "$KEYS_DIR"
AOSP_SEC_URL="https://android.googlesource.com/platform/build/+/refs/heads/main/target/product/security"
for kf in platform.pk8 platform.x509.pem; do
  if [[ ! -f "$KEYS_DIR/$kf" ]]; then
    echo "Downloading $kf from AOSP Gitiles ..."
    curl -fsSL "$AOSP_SEC_URL/$kf?format=TEXT" | python3 -c 'import base64,sys; sys.stdout.buffer.write(base64.b64decode(sys.stdin.read()))' > "$KEYS_DIR/$kf"
  fi
done
CERT_SHA="$(openssl x509 -in "$KEYS_DIR/platform.x509.pem" -outform der | shasum -a 256 | awk '{print $1}')"
if [[ "$CERT_SHA" != "c8a2e9bccf597c2fb6dc66bee293fc13f2fc47ec77bc6b2b0d52c11f51192ab8" ]]; then
  echo "Error: Unexpected AOSP platform.x509.pem SHA-256: $CERT_SHA" >&2
  exit 1
fi

(cd "$WORK_SAVER/dex" && zip -9 -X "$WORK_SAVER/res_unsigned.apk" classes.dex)
"$BT_DIR/zipalign" -p -f 4 "$WORK_SAVER/res_unsigned.apk" "$WORK_SAVER/aligned.apk"
"$BT_DIR/apksigner" sign \
  --key "$KEYS_DIR/platform.pk8" \
  --cert "$KEYS_DIR/platform.x509.pem" \
  --out "$WORK_SAVER/PersonalScreenSaver.apk" \
  "$WORK_SAVER/aligned.apk"

echo "Built APK (in temp dir): $WORK_SAVER/PersonalScreenSaver.apk"

if [[ -n "$PROJ_IP" ]]; then
  TARGET="${PROJ_IP}:5555"
  adb connect "$TARGET"
  adb -s "$TARGET" install -r -d "$WORK_SAVER/PersonalScreenSaver.apk"
  adb -s "$TARGET" shell "cmd package compile -m speed -f com.ngantla.personalscreensaver"
  adb -s "$TARGET" shell "settings put secure screensaver_components com.ngantla.personalscreensaver/.PhotoDreamService && settings put secure screensaver_default_component com.ngantla.personalscreensaver/.PhotoDreamService && am startservice -n com.ngantla.personalscreensaver/.HtpSaverWatcherService"
  rm -rf "$WORK_SAVER"
  echo "Installed and activated PersonalScreenSaver on $TARGET!"
fi
