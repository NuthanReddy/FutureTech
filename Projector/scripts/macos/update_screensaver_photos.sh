#!/usr/bin/env zsh
# Batch-crops a folder of photos to 1920x1080 on macOS (Pillow) and syncs them
# to /sdcard/Screensaver + /Reserve0/screensaver on the projector.
# Usage: ./update_screensaver_photos.sh ~/Pictures/ProjectorSlideshow [PROJECTOR_IP]

set -euo pipefail

SRC_PHOTOS="${1:-$HOME/Pictures/ProjectorSlideshow}"
PROJ_IP="${2:-}"
PROJ_MAC="${PROJ_MAC:-3c:be:8e:43:78:87}"

if [[ ! -d "$SRC_PHOTOS" ]]; then
  echo "Error: Folder not found at '$SRC_PHOTOS'. Usage: ./update_screensaver_photos.sh ~/Pictures/Photos [ip]" >&2
  exit 1
fi

if [[ -z "$PROJ_IP" ]]; then
  ping -c 2 255.255.255.255 >/dev/null 2>&1 || true
  PROJ_IP="$(arp -an | grep -Ei "$PROJ_MAC" | sed -E 's/.*\(([0-9.]+)\).*/\1/' | head -n 1 || true)"
  [[ -n "$PROJ_IP" ]] || { echo "Error: Pass PROJECTOR_IP as 2nd argument." >&2; exit 1; }
fi

STAGE_DIR="$(mktemp -d /tmp/saver_photos.XXXXXX)"
trap 'rm -rf "$STAGE_DIR"' EXIT

python3 - "$SRC_PHOTOS" "$STAGE_DIR" <<'PY'
import os, re, sys
from PIL import Image, ImageOps

src_dir, dst_dir = sys.argv[1], sys.argv[2]
files = sorted(f for f in os.listdir(src_dir) if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")))
if not files:
    raise SystemExit("No .jpg/.jpeg/.png/.webp files found in " + src_dir)
for idx, name in enumerate(files, 1):
    im = ImageOps.exif_transpose(Image.open(os.path.join(src_dir, name))).convert("RGB")
    fitted = ImageOps.fit(im, (1920, 1080), method=Image.Resampling.LANCZOS, centering=(0.5, 0.38))
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.splitext(name)[0])
    out_name = f"{idx:02d}_{stem}.jpg"
    fitted.save(os.path.join(dst_dir, out_name), "JPEG", quality=95, subsampling=0)
    print("Prepared:", out_name)
PY

TARGET="${PROJ_IP}:5555"
adb connect "$TARGET"
adb -s "$TARGET" shell "mkdir -p /sdcard/Screensaver /Reserve0/screensaver && rm -f /sdcard/Screensaver/* /Reserve0/screensaver/*"

for f in "$STAGE_DIR"/*.jpg; do
  bn="$(basename "$f")"
  adb -s "$TARGET" push "$f" "/data/local/tmp/$bn"
  adb -s "$TARGET" shell "cat /data/local/tmp/$bn > /sdcard/Screensaver/$bn && cat /data/local/tmp/$bn > /Reserve0/screensaver/$bn && rm -f /data/local/tmp/$bn"
done

adb -s "$TARGET" shell "sync && am start -n com.ngantla.personalscreensaver/.PersonalSlideshowActivity"
echo "Screensaver slideshow updated and launched!"
