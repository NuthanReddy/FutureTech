#!/usr/bin/env zsh
# Generates a 1920x1080 STORE-only bootanimation.zip and exact 6,220,854-byte 24-bit BGR
# BOOTLOGO.BMP on macOS (Pillow), flashes both safely, and verifies SHA-256 on the projector.
# Usage: ./set_boot_screen.sh ~/Pictures/photo.jpg [CROP_TOP_Y] [PROJECTOR_IP]

set -euo pipefail

PHOTO_PATH="${1:-$HOME/Pictures/photo.jpg}"
CROP_TOP_Y="${2:-850}"
PROJ_IP="${3:-}"
PROJ_MAC="${PROJ_MAC:-3c:be:8e:43:78:87}"

if [[ ! -f "$PHOTO_PATH" ]]; then
  echo "Error: Photo not found at '$PHOTO_PATH'. Usage: ./set_boot_screen.sh ~/Pictures/photo.jpg [crop_top_y] [ip]" >&2
  exit 1
fi

if [[ -z "$PROJ_IP" ]]; then
  ping -c 2 255.255.255.255 >/dev/null 2>&1 || true
  PROJ_IP="$(arp -an | grep -Ei "$PROJ_MAC" | sed -E 's/.*\(([0-9.]+)\).*/\1/' | head -n 1 || true)"
  [[ -n "$PROJ_IP" ]] || { echo "Error: Pass PROJECTOR_IP as 3rd argument." >&2; exit 1; }
fi

OUT_DIR="$(mktemp -d /tmp/proj_boot.XXXXXX)"
trap 'rm -rf "$OUT_DIR"' EXIT

python3 - "$PHOTO_PATH" "$CROP_TOP_Y" "$OUT_DIR" <<'PY'
import os, sys, zipfile
from PIL import Image, ImageOps

src_path, crop_top_y, out_dir = sys.argv[1], int(sys.argv[2]), sys.argv[3]
im = ImageOps.exif_transpose(Image.open(src_path)).convert("RGB")
w, h = im.size
crop_h = round(w * 9 / 16)
top = max(0, min(crop_top_y, h - crop_h))
framed = im.crop((0, top, w, top + crop_h)).resize((1920, 1080), Image.Resampling.LANCZOS)

jpg_path = os.path.join(out_dir, "01.jpg")
framed.save(jpg_path, "JPEG", quality=95, subsampling=0)

zip_path = os.path.join(out_dir, "bootanimation.zip")
with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
    zf.writestr("desc.txt", b"1920 1080 15\np 0 0 part0\n")
    zf.write(jpg_path, "part0/01.jpg")

hdr = bytes.fromhex(
    "424d36ec5e00000000003600000028000000800700003804000001001800"
    "0000000000ec5e00c40e0000c40e00000000000000000000"
)
rgb = framed.tobytes()
bmp = bytearray(6220854)
bmp[:54] = hdr
stride = 1920 * 3
for y in range(1080):
    src_off = (1079 - y) * stride
    dst_off = 54 + y * stride
    row_rgb = rgb[src_off : src_off + stride]
    bmp[dst_off + 0 : dst_off + stride : 3] = row_rgb[2::3]
    bmp[dst_off + 1 : dst_off + stride : 3] = row_rgb[1::3]
    bmp[dst_off + 2 : dst_off + stride : 3] = row_rgb[0::3]

bmp_path = os.path.join(out_dir, "custom_bootlogo.bmp")
open(bmp_path, "wb").write(bmp)
print("Generated:", bmp_path, len(bmp), "bytes &", zip_path)
PY

BMP_SHA="$(shasum -a 256 "$OUT_DIR/custom_bootlogo.bmp" | awk '{print $1}')"
ZIP_SHA="$(shasum -a 256 "$OUT_DIR/bootanimation.zip" | awk '{print $1}')"

TARGET="${PROJ_IP}:5555"
adb connect "$TARGET"
adb -s "$TARGET" push "$OUT_DIR/bootanimation.zip" "/data/local/tmp/bootanimation.zip"
adb -s "$TARGET" push "$OUT_DIR/custom_bootlogo.bmp" "/data/local/tmp/custom_bootlogo.bmp"

adb -s "$TARGET" shell '
cat /data/local/tmp/bootanimation.zip > /Reserve0/bootanimation.zip &&
dd if=/dev/block/mmcblk0 of=/data/local/tmp/bl_tail.bin bs=512 skip=138983 count=1 2>/dev/null &&
cat /data/local/tmp/custom_bootlogo.bmp > /data/local/tmp/custom_bootlogo_padded.bmp &&
dd if=/data/local/tmp/bl_tail.bin bs=1 skip=54 count=458 2>/dev/null >> /data/local/tmp/custom_bootlogo_padded.bmp &&
dd if=/data/local/tmp/custom_bootlogo_padded.bmp of=/dev/block/mmcblk0 bs=512 seek=126833 count=12151 conv=notrunc,fsync &&
sync &&
rm -f /data/local/tmp/bootanimation.zip /data/local/tmp/custom_bootlogo*.bmp /data/local/tmp/bl_tail.bin
'

DEV_BMP_SHA="$(adb -s "$TARGET" shell "dd if=/dev/block/mmcblk0 bs=512 skip=126833 count=12151 2>/dev/null | head -c 6220854 | sha256sum" | tr -d '\r' | awk '{print $1}')"
DEV_ZIP_SHA="$(adb -s "$TARGET" shell "sha256sum /Reserve0/bootanimation.zip" | tr -d '\r' | awk '{print $1}')"

if [[ "$BMP_SHA" != "$DEV_BMP_SHA" || "$ZIP_SHA" != "$DEV_ZIP_SHA" ]]; then
  echo "Error: SHA-256 verification failed!" >&2
  exit 1
fi
echo "Boot splash + Android bootanimation verified ($BMP_SHA)!"
