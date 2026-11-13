# WZATCO Yuva Blaze — Reverse-Engineering, Turbo Alcove Alignment & DIY Customization Archive

Complete reverse-engineering research, custom on-device computer-vision alignment code, standalone automation scripts, OEM firmware/APK static-analysis dumps, calibration plots, and sanitized engineering session logs for the **WZATCO Yuva Blaze** (`Allwinner H727` / `sun50iw15p1`, Android 14 `armeabi-v7a`, `1920×1080` BOE LCD) smart projector.

> **Privacy & Safety Note**: All personal photographs, personal screensaver APKs, home wall captures, credentials, and raw `userdata` contents are strictly excluded from this repository. Standalone boot-screen and screensaver helper scripts use generic placeholder image paths.

---

## Quick Links — DIY Manuals & Design Docs

| Document | Description |
|---|---|
| **[Windows DIY & Engineering Guide](./MANUAL_UPGRADE_GUIDE_WINDOWS.md)** | Complete reverse-engineering writeup and copy-pasteable PowerShell manual for Windows (`adb`, full eMMC backup, rebuilding/deploying the 3-phase alignment APK, custom `BOOTLOGO.BMP` + `bootanimation.zip`, and custom slideshow screensaver). |
| **[macOS DIY & Debugging Guide](./MANUAL_UPGRADE_GUIDE_MACOS.md)** | Native macOS (`zsh`, Apple Silicon & Intel) counterpart covering Homebrew Android SDK/Temurin setup, BSD `arp` discovery, `bridge100` `tcpdump` telemetry capture, Python/Pillow `ZIP_STORED` + bottom-up BGR `BOOTLOGO.BMP` generation, live `Preview` inspection, and APK builds. |
| **[Initial Engineering Plan (`plan.md`)](./plan.md)** | Architectural plan for low-RAM on-device object avoidance and sharpness-preserving 16:9 auto-resize. |
| **[Session Checkpoints (`checkpoints/`)](./checkpoints/index.md)** | Detailed chronological engineering checkpoint ([`001-fast-alcove-sidewall-avoidance.md`](./checkpoints/001-fast-alcove-sidewall-avoidance.md)) documenting hardware discoveries and calibration iterations. |

---

## Hardware & Upgrade Highlights

- **Platform**: Allwinner H727 (`sun50iw15p1`), 4× ARM Cortex-A53 (`armeabi-v7a`), 1 GB RAM, Android 14 `userdebug` (`A series_0.0.1_2604240125`), SELinux `Permissive`, signed with the public AOSP `platform` testkey.
- **AVB-Safe `/data/app` Subclassing**: Stock `com.hysd.screen.apk` (`com.hysd.hyscreen`) has `Lcom/hysd/hyscreen/VAF2S;` binary-patched to `VAF2T` in `classes.dex` so `classes3.dex` supplies `public class VAF2S extends VAF2T`—preserving untouched `/system` (`vbmeta_system`) with instant one-command rollback (`adb uninstall com.hysd.hyscreen` or `setprop persist.hysd.turbo_disable 1`).
- **4.2× Faster 3-Phase Alignment (`16.1 s` → `3.75 s` on repoints)**:
  1. **Phase 1 — 3D Tsai Stereo Trapezoid (`~2.7 s`)**: Executes stock `VAF2T` (`afswitch=0, dmode=0`) for IMU + stereo perspective homography $\mathbf{H}_{\text{trap}}$ and throw distance (`result_angle[9]` mm).
  2. **Phase 2 — Constant-50%-Flux Checkerboard Alcove & Obstacle Solver (`~0.78 s`)**: Projects complementary `20×12` checkerboards + L-triad origin tag (`RGB(188,188,188)` vs `RGB(0,0,0)`) inside `baseTrap` with zero UVC auto-exposure drift, closed-loop V4L2 pixel-inversion gating, $O(1)$ van Herk max-filters, 209-corner RANSAC homography, bright-plateau alcove sidewall detection, and monotonic-stack strict-`16:9` fitting composed via $\mathbf{H}_{\text{trap}} \circ \mathbf{S}_{\text{crop}}$.
  3. **Phase 3 — Smart Distance-Gated Post-Crop Autofocus**: Fixes stock `motorbegin` thread leak (`motorthreadstop(-3)`) and focuses strictly inside the cropped `16:9` target wall when cold-booting or when throw distance shifts `> 4.5%`.
- **Dual-Stage Custom Boot Screen**: Exact `6,220,854`-byte 24-bit bottom-up BGR `BOOTLOGO.BMP` spliced into `bootloader_a` (`LBA 126,833..138,983`, preserving the trailing `458` cluster bytes) paired with a `STORE`-only `/Reserve0/bootanimation.zip`.
- **Crossfading Slideshow Screensaver Source (`personal_saver_app/`)**: Lightweight system-UID companion app (`PhotoDreamService` + `HtpSaverWatcherService`) that intercepts stock `Htp_ScreenSaver` (`sys.lmk_top_app`) and crossfades `1920×1080` images from `/sdcard/Screensaver` (`/Reserve0/screensaver` pre-unlock fallback).

---

## Directory Structure

```text
Projector/
├── README.md                              # Folder overview and index
├── MANUAL_UPGRADE_GUIDE_WINDOWS.md        # Windows PowerShell DIY & reverse-engineering manual
├── MANUAL_UPGRADE_GUIDE_MACOS.md          # macOS zsh/Python DIY & debugging manual
├── plan.md                                # Initial technical design plan
├── checkpoints/                           # Engineering session checkpoints
│   ├── index.md
│   └── 001-fast-alcove-sidewall-avoidance.md
├── alcove-calibration-result.png          # Live Run 8 alcove 16:9 fit diagnostic output
├── stock_bootlogo.bmp                     # Factory WZATCO 1920x1080 24-bit BGR boot splash
├── stock_bootlogo_preview.jpg             # JPEG preview of stock boot splash
├── alcove_upgrade/                        # Custom 3-phase alignment & alcove avoidance upgrade
│   ├── src/com/hysd/hyscreen/             # VAF2S.java, AlcoveVisionPipeline.java, HomographySolver.java, PatternOverlayView.java
│   ├── stubs/                             # Compile-time stubs (VAF2T, HYSDVAF2, HYSDInterface, SystemProperties)
│   ├── build/                             # Compiled jars, patched classes.dex + classes3.dex, signed com.hysd.screen-turbo-alcove.apk
│   └── alcove_debug_run*.png              # On-device synthetic vision diagnostic plots (Runs 1–11)
├── personal_saver_app/                    # Pure source code for the custom slideshow screensaver app
│   ├── AndroidManifest.xml
│   └── src/com/ngantla/personalscreensaver/
├── scripts/                               # Standalone helper scripts (placeholder paths)
│   ├── windows/                           # PowerShell (.ps1): backup, build, boot screen, screensaver sync
│   └── macos/                             # zsh (.sh): backup, build, boot screen, screensaver sync, live debug
├── projector_artifacts/                   # Pulled stock OEM APKs, PQ/display configs, REA JSON evidence, and dexdumps
│   ├── *.apk                              # Stock com.hysd.screen.apk, com.hysd.client.apk, Htp_Jetter.apk, Htp_Motor.apk, etc.
│   ├── configs/                           # Factory display_cfg.ini, pq_*.ini, tvpq.db
│   ├── rea_evidence/                      # Static reverse-engineering evidence manifests (REA v6.7.0)
│   └── extracted/                         # Unpacked APKs, native .so libraries, and dexdump outputs (.gz for >45 MB dumps)
├── screensaver_inspect/                   # Unpacked stock Htp_ScreenSaver resources, manifest, and gzipped dexdump
└── session_logs/                          # Sanitized engineering session telemetry
    ├── session_todos.json                 # Exported task & dependency graph from session.db
    └── events_sanitized.jsonl             # Sanitized agent/tool engineering trace (system blocks, images & personal strings stripped)
```

---

## Standalone Helper Scripts (`scripts/`)

| Windows (`scripts/windows/`) | macOS (`scripts/macos/`) | Purpose |
|---|---|---|
| `backup_projector.ps1` | `backup_projector.sh` | Locate projector by MAC over Wi-Fi/hotspot, pull `/sdcard/focus*` Tsai calibration, dump raw eMMC partitions, and verify SHA-256 checksums. |
| `build_alcove_upgrade.ps1` | `build_alcove_upgrade.sh` | Compile Java stubs + `VAF2S`/`AlcoveVisionPipeline`, patch `classes.dex` (`VAF2S` → `VAF2T`), auto-fetch & verify Google's public AOSP `platform` key (`c8a2e9bc…` into gitignored `alcove_upgrade/keys/`), sign, install to `/data/app`, and AOT-compile (`-m speed`). |
| `set_boot_screen.ps1` | `set_boot_screen.sh` | Crop a `16:9` `1920×1080` image, generate `STORE`-only `bootanimation.zip` + exact `6,220,854`-byte 24-bit BGR `BOOTLOGO.BMP`, splice into `LBA 126,833..138,983`, and verify SHA-256. |
| `update_screensaver_photos.ps1` | `update_screensaver_photos.sh` | Sync `1920×1080` slideshow images to `/sdcard/Screensaver` and `/Reserve0/screensaver` (plus optional source build in `build_personal_screensaver.sh`). |

---

## Compressed Large Dexdumps (`>= 45 MB` Gzipped for GitHub Compatibility)

To keep every file in the repository well below GitHub's file-size limit, plain-text `dexdump` disassemblies `>= 45 MB` are compressed with `gzip` (`.gz`):

- `projector_artifacts/extracted/Htp_Setting/classes.dex.txt.gz` (uncompressed: `140.7 MB`)
- `projector_artifacts/extracted/Htp_Jetter/classes.dump.txt.gz` (uncompressed: `131.2 MB`)
- `projector_artifacts/extracted/com.hysd.screen/dexdump_d.txt.gz` (uncompressed: `90.4 MB`)
- `screensaver_inspect/classes.dump.txt.gz` (uncompressed: `58.2 MB`)

Decompress locally on macOS/Linux (`gunzip -k <file>.gz`) or Windows/Python (`python -m gzip -d <file>.gz`).
