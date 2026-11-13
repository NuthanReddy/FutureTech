#!/usr/bin/env zsh
# Connects to the WZATCO Yuva Blaze projector on macOS and performs a SHA-256-verified
# backup of factory Tsai optical calibration (/sdcard/focus*) and raw eMMC partitions.

set -euo pipefail

PROJ_IP="${1:-}"
PROJ_MAC="${PROJ_MAC:-3c:be:8e:43:78:87}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/Backups/Wzatco-Yuva-Blaze}"

if [[ -z "$PROJ_IP" ]]; then
  ping -c 2 255.255.255.255 >/dev/null 2>&1 || true
  # BSD arp strips leading zeros on hex bytes; match flexibly if needed
  PROJ_IP="$(arp -an | grep -Ei "$PROJ_MAC" | sed -E 's/.*\(([0-9.]+)\).*/\1/' | head -n 1 || true)"
  if [[ -z "$PROJ_IP" ]]; then
    echo "Error: Could not auto-discover projector MAC ($PROJ_MAC). Pass IP as first arg: ./backup_projector.sh <IP>" >&2
    exit 1
  fi
fi

TARGET="${PROJ_IP}:5555"
echo "Connecting to projector at $TARGET ..."
adb connect "$TARGET"

mkdir -p "$BACKUP_DIR/calibration" "$BACKUP_DIR/partitions"

echo "1. Backing up unit-unique factory Tsai calibration (/sdcard/focus & /sdcard/focus_d) ..."
adb -s "$TARGET" pull /sdcard/focus "$BACKUP_DIR/calibration/focus"
adb -s "$TARGET" pull /sdcard/focus_d "$BACKUP_DIR/calibration/focus_d"

dump_and_verify() {
  local name="$1" skip="$2" count="$3"
  local out="$BACKUP_DIR/partitions/$name"
  echo "2. Dumping $name (LBA $skip, $count sectors) ..."
  adb -s "$TARGET" exec-out "dd if=/dev/block/mmcblk0 bs=512 skip=$skip count=$count 2>/dev/null" > "$out"

  local mac_sha dev_sha
  mac_sha="$(shasum -a 256 "$out" | awk '{print $1}')"
  dev_sha="$(adb -s "$TARGET" shell "dd if=/dev/block/mmcblk0 bs=512 skip=$skip count=$count 2>/dev/null | sha256sum" | tr -d '\r' | awk '{print $1}')"
  if [[ "$mac_sha" != "$dev_sha" ]]; then
    echo "Error: SHA-256 mismatch for $name! Mac=$mac_sha vs Device=$dev_sha" >&2
    exit 1
  fi
  echo "   Verified $name SHA-256: $mac_sha"
}

dump_and_verify "factorydata.img"  73728   32768
dump_and_verify "bootloader_a.img" 106496  65536
dump_and_verify "Reserve0_a.img"   7422976 32768

echo "Backup completed and verified in: $BACKUP_DIR"
