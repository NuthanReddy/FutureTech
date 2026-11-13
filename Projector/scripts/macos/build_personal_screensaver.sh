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

(cd "$WORK_SAVER/dex" && zip -9 -X "$WORK_SAVER/res_unsigned.apk" classes.dex)
"$BT_DIR/zipalign" -p -f 4 "$WORK_SAVER/res_unsigned.apk" "$WORK_SAVER/aligned.apk"
"$BT_DIR/apksigner" sign \
  --key "$REPO_PROJ/alcove_upgrade/keys/platform.pk8" \
  --cert "$REPO_PROJ/alcove_upgrade/keys/platform.x509.pem" \
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
