<overview>
The user owns a WZATCO Yuva Blaze Android 14 projector (`Allwinner H727` / `sun50iw15p1`, 32-bit `armeabi-v7a`, 1 GB RAM) and initially asked to upgrade its software, understand outbound telemetry, and patch vulnerabilities, then prioritized backing up the entire projector software/firmware and upgrading its on-device obstacle avoidance and auto-resize to detect recessed alcove side walls and run much faster. We completed a bit-for-bit SHA-256-verified 4.48 GiB full eMMC/APK/calibration backup on the PC, reverse-engineered the stock `HYSD`/`HTP` projection stack using REA (`rea@6.7.0`) and Android SDK tools, and built, signed, AOT-compiled, and live-verified a drop-in `/data/app` upgrade to `com.hysd.hyscreen` (`VAF2S` + `AlcoveVisionPipeline`) that locks onto the bright flat alcove wall in **1.49 s** (vs. ~5 s stock) without touching `/system`. We are currently answering the user's follow-up questions on how automatic alignment behaves on bootup and when repointing the projector.
</overview>

<history>
1. **Initial request: software upgrade, outbound traffic audit, and vulnerability patching**
   - Ran initial `security-review` assessment, checked official WZATCO support channels, inspected TP-Link `Archer-C5` router capabilities, and connected the projector to the Windows PC's Mobile Hotspot (`192.168.137.x`).
   - Checked Windows `pktmon` (occupied by `SenseNdr` Defender counters) and introduced `PCAPdroid` as an optional local no-root traffic monitor.

2. **Pivot to firmware analysis, object detection, and edge-blur improvements (`reverse-engineer-anything` skill)**
   - Connected to the projector over open network ADB (`:5555`, MAC `3C-BE-8E-43-78-87`).
   - Used `adb` and `rea@6.7.0` (`inspect-adb-device`, `inspect-adb-display`, `inspect-artifact`) plus `aapt2`/`dexdump` to analyze OEM packages (`com.hysd.screen.apk`, `com.hysd.client.apk`, `Htp_Jetter.apk`, `HtpInterfaces.apk`, `Htp_Motor.apk`, `Htp_Setting.apk`, `AwTvFactoryMenu.apk`) and PQ configs (`tvpq.db`, `pq_picturemode.ini`).
   - Identified why edges were blurry (active `persist.display.keystone_*` insets of `(320, 160)` discarding 53% of pixels + `snr=1, tnr=2`) and briefed the user on how stock `HYSD` (`VAF2S` + `libfocusvahysd.so`) performs `640x480` OpenCV structured-light contour obstacle avoidance.

3. **Planning documentation & recessed alcove side-wall problem**
   - In `[[PLAN]]` mode, created `plan.md` to document faster on-device object avoidance; plan was approved.
   - User shared a photo of their recessed alcove wall showing projection spilling left onto a protruding pillar and right across a 90° crease onto a side wall + switchboard (noting side-wall text also appears blurrier), asking how obstacle avoidance can auto-detect side walls. Explained structured-light homography kinks, Lambertian $\cos\theta$ irradiance steps, and Depth-from-Defocus (DfD).

4. **Full backup of projector apps, calibration, configs, and firmware partitions**
   - User requested: *"before you do anything. backup all the apps and software of the projector on this pc"*.
   - Backed up all 7 unit-unique `/sdcard/focus*` calibration files, all 119 installed APK/APEX packages (`898.79 MiB`), 986 `/etc` config files (`etc_configs.tar`), metadata/settings dumps, and raw eMMC images via world-readable `/dev/block/mmcblk0` (`boot0`/SPL, primary/backup GPT, `factorydata`, `private`, `bootloader_a/b`, `boot_a/b`, `vendor_boot_a/b`, `init_boot_a/b`, `dtbo_a/b`, `vbmeta*`, `Reserve0_a/b`, and 3.00 GiB `super.img`, excluding raw `userdata` for privacy) to `C:\Users\ngantla\Backups\Wzatco-Yuva-Blaze` (`4.48 GiB`, 170 files), verifying every SHA-256 against the live device.

5. **Implementing and live-tuning faster side-wall & obstacle avoidance**
   - User requested: *"now update the object avoidance to detect sidewalls and faster"*.
   - Discovered `com.hysd.screen.apk` (`com.hysd.hyscreen`, `android.uid.system`) is signed with the standard AOSP `platform` testkey (`SHA-256: c8a2e9bccf597c2fb6dc66bee293fc13f2fc47ec77bc6b2b0d52c11f51192ab8`), SELinux is `Permissive`, `HYSDVAF2.s0cam(0,2)`/`s1cam(byte[614400])`/`s2cam()` captures raw `640x480` YUYV422 frames from `/dev/video0`, and `com.hysd.client.VAFclientapi` (`HYSDInterface.setkey`) applies keystone insets.
   - Renamed stock `Lcom/hysd/hyscreen/VAF2S` -> `VAF2T` in `classes.dex` and compiled a drop-in subclass `com.hysd.hyscreen.VAF2S extends VAF2T` + `AlcoveVisionPipeline` into `classes3.dex`, installed non-destructively to `/data/app` via `adb install -r -d` + `cmd package compile -m speed`.
   - Iterated across 8 live hardware calibration runs using diagnostic `/sdcard/alcove_debug.png` captures and user visual feedback:
     - Fixed Android 12+ `FLAG_NOT_TOUCHABLE` 80% alpha dimming by removing `FLAG_NOT_TOUCHABLE`.
     - Eliminated 255 CMOS blooming and UVC Auto-Exposure gain swings by designing a **constant-50%-flux 3-frame complementary checkerboard** (`MODE_CHECKER_POS`, `MODE_CHECKER_NEG`, `MODE_ORIGIN_TAG` at `RGB(188,188,188)` vs `RGB(0,0,0)`) with closed-loop pixel-flip V4L2 buffer gating.
     - Fixed radial webcam barrel distortion false positives and global $1/r^2$ wall-shading cutoffs, enforced an ironclad `16:9` aspect lock on every code path, and locked onto the bright central alcove plateau (`meanIn / meanOut >= 1.17`, `coherence >= 0.78`).
   - Run 8 succeeded in **1,493 ms** (`1,096 ms` capture + `396 ms` solve), locking **170 checkerboard corners**, detecting `Walls L=320 R=1590`, and applying `setkey=[340, 352, 340, 42, 360, 42, 360, 352]` (`1220x686` exact `16:9` centered on the bright alcove wall). Published `alcove-calibration-result.png` via `publish_chat_update`; user confirmed: *"cool. now it works"*.
   - Archived the signed APK and Java source to `C:\Users\ngantla\Backups\Wzatco-Yuva-Blaze\custom_upgrades` and set `persist.hysd.alcove.debug=0` on the projector (`192.168.137.118:5555`).

6. **Follow-up questions on bootup & repointing behavior**
   - User asked: *"does this automatically align on bootup?"* and *"how does it work when i repoint the projector?"*.
   - Inspected `Htp_Jetter` (`BootReceiver`, `MonitorControlManager`, `FocusMonitorService`, `AfUtils`) and live `getprop` on `192.168.137.118:5555` just before compaction.
</history>

<work_done>
Files created/updated:
- `C:\Users\ngantla\Backups\Wzatco-Yuva-Blaze\*`: Complete 4.48 GiB (170 files) verified backup including `SHA256SUMS.txt`, `BACKUP_README.md`, `calibration\focus*`, `apks\*` (119 packages), `configs\etc_configs.tar`, `metadata\*`, `partitions\*.img` (`super.img`, `factorydata.img`, `boot_a/b.img`, etc.), and `custom_upgrades\com.hysd.screen-turbo-alcove.apk` + `custom_upgrades\src\*`.
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\VAF2S.java`: Upgraded `VAF2S` service extending `VAF2T` with parallel camera/UI startup, closed-loop V4L2 frame-flip capture, center-ROI back-wall autofocus, optional 2-step motor DfD bracket, and runtime fallback (`persist.hysd.turbo_disable=1`).
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\alcove\PatternOverlayView.java`: Constant-50%-flux 20x12 complementary checkerboard + L-triad origin tag (`(10,6)`) renderer (`1920x1080`, VSync latch).
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\alcove\AlcoveVisionPipeline.java`: 209-corner saddle detector, parity-checked BFS lattice indexer, RANSAC homography solver, `maxFilter13x13(|yPos - yNeg|)` radiance envelope, bright-plateau vertical/horizontal seam detector, local-background obstacle mask, and $O(GW \times GH)$ monotonic-stack strict-16:9 solver.
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\alcove\HomographySolver.java`: Hartley-normalized DLT + RANSAC 3x3 planar homography solver.
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove-calibration-result.png`: Published diagnostic image from live Run 8.

Work completed:
- [x] Reverse-engineered projector hardware, camera/motor/keystone HALs, and stock `HYSD`/`HTP` packages with REA Evidence IDs (`ev_355c164e…`, `ev_b6f9165e…`, `ev_74be7926…`, `ev_380c264c…`, `ev_0cf5c441…`, `ev_7ac16f73…`, `ev_103bd1c5…`).
- [x] Completed and SHA-256-verified full 4.48 GiB projector backup on PC.
- [x] Built, deployed (`/data/app`), AOT-compiled, and live-verified upgraded `com.hysd.hyscreen` fast sidewall + obstacle avoidance (`1.49 s`, `1220x686` 16:9 fit on bright alcove wall; confirmed working by user).
- [ ] Finish checking `FocusMonitorService` / `Htp_hui2` bootup trigger details and answer the user's question on bootup alignment + repointing behavior.
- [ ] (Deferred earlier by user) Optional network telemetry audit & vulnerability hardening (e.g., open port `5555` ADB, `Htp_Mac_Tracker`, `Zeasn` services) and/or standalone Library design doc if still desired.
</work_done>

<technical_details>
- **Hardware & OS**: WZATCO Yuva Blaze (`H727MA3ZeasnBOE400enwzatco1`), Allwinner H727 (`sun50iw15p1`), 4x ARMv7 Cortex-A53 (32-bit only: `armeabi-v7a`), 977 MB RAM (~169 MB available), BOE `1920x1080` LCD, Android 14 `userdebug` (`A series_0.0.1_2604240125`), SELinux `Permissive`. Current hotspot IP: `192.168.137.118:5555` (MAC `3C:BE:8E:43:78:87`).
- **Camera & Native Library**: `config.disable_cameraservice=true` disables Android Camera2. `/dev/video0` (`1bdf:0b78` USB UVC webcam) is accessed via V4L2 inside `libfocusvahysd.so` (`HYSDVAF2.s0cam(0, 2)` opens stream, `s1cam(byte[614400])` dequeues `640x480` YUYV422 where even bytes `yuyv[2*i]` are 8-bit luma, `s2cam()` releases).
- **V4L2 Ring Buffer & UVC Auto-Exposure Gotchas**:
  - `HYCAM` allocates 4 V4L2 mmap ring buffers, and `OpenGLRenderer` takes ~150 ms on first surface gralloc. Switching between dark (`0`) and bright (`255`) frames causes huge UVC Auto-Exposure (AE) gain swings and 255 CMOS saturation.
  - Using **constant-50%-flux complementary checkerboards** (`RGB(188,188,188)` vs `RGB(0,0,0)`) keeps UVC AE completely steady, cancels ambient light and room interreflection (`W = maxFilter13x13(|yPos - yNeg|) == 0..2` outside beam), and lets `captureFlippedCheckerFrame` gate closed-loop on actual pixel inversion (`meanFlip >= minFlipDiff`).
  - Android 12+ `WindowManager` forces `alpha = 0.80` on `TYPE_APPLICATION_OVERLAY` windows if `FLAG_NOT_TOUCHABLE` is set; omitting `FLAG_NOT_TOUCHABLE` preserves `alpha = 1.00`.
- **Keystone Coordinate Convention**:
  - `com.hysd.client.VAFclientapi` (`exported="true"`, `HYSDInterface.setkey(ltx, lty, lbx, lby, rbx, rby, rtx, rty)`) passes 8 non-negative inward pixel offsets to `AwTvDisplayManager.setKeystoreValue` -> `vendor.display.config@1.0::IDisplayConfig`:
    `TL = (ltx, lty)`, `BL = (lbx, 1079 - lby)`, `BR = (1919 - rbx, 1079 - rby)`, `TR = (1919 - rtx, rty)`.
- **DEX Subclassing & Instant Rollback**:
  - System apps are signed with the public AOSP `platform` key (`SHA-256: c8a2e9bccf597c2fb6dc66bee293fc13f2fc47ec77bc6b2b0d52c11f51192ab8`).
  - Replacing `Lcom/hysd/hyscreen/VAF2S` -> `Lcom/hysd/hyscreen/VAF2T` (38 occurrences in `classes.dex`, preserving exact string length and sort order, with recomputed SHA-1/Adler32) allows `classes3.dex` to provide `public class VAF2S extends VAF2T`.
  - Runtime fallback: `setprop persist.hysd.turbo_disable 1` delegates back to stock `VAF2T`. Full rollback to untouched `/system/priv-app/com.hysd.screen/com.hysd.screen.apk`: `adb uninstall com.hysd.hyscreen`.
  - Tunable system properties: `persist.hysd.alcove.min_area` (default `"0.24"`), `persist.hysd.alcove.dfd` (`"0"` or `"1"` for 2-step focus-motor DfD), `persist.hysd.alcove.hud` (`"1"`), `persist.hysd.alcove.debug` (currently `"0"`).
- **Boot & Repoint Architecture (`Htp_Jetter` / `Htp_Splash`)**:
  - Live properties verified on device: `persist.sys.poweron.autofocus=1`, `persist.sys.autofocus=1`, `persist.sys.autotrapezoid=1`, `persist.htp.hysd.dmode=3`, and process `com.htp.app.jetter:focus_monitor` (PID 1492) is running.
  - `BootReceiver` starts `FocusMonitorService` on `LOCKED_BOOT_COMPLETED` / `BOOT_COMPLETED`. `FocusMonitorService` runs `DataDriftChecker` on a `20 ms` `ScheduledExecutorService` polling IMU sensor data (`updateSensorData`), and when movement + stillness settles, calls `AfUtils.startFocus()` -> `startService(Intent(com.hysd.hyscreen/.VAF2S, mode=1, afswitch=1, keyswitch=1, dmode=3))`.
  - In our `VAF2S.onStartCommand`, `mode=1` with `afswitch=1, keyswitch=1, dmode=3` matches `isTurboCandidate = true` and `runCenterAfFirst = true`—so it runs `runFastCenterBackWallAutofocus` on the central `160x120` back-wall ROI followed immediately by `solveChecker(...)`! (Need to do a quick 1-read check of `FocusMonitorService.onStartCommand`/`updateSensorData` and `Htp_Splash` to confirm whether bootup itself fires an initial `startFocus()` on cold boot or if `Htp_Splash` triggers `VAF2S` on power-on.)
</technical_details>

<important_files>
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\VAF2S.java`
  - Drop-in `com.hysd.hyscreen.VAF2S` service intercepting `mode` (`1, 3, 4, 5, 6, 7, 20, 21, 99`) for fast center-ROI autofocus + 3-frame complementary checkerboard capture.
  - Key sections: `onStartCommand` (lines 42–108), `executeTurboAlcovePipeline` (lines 125–285), `runFastCenterBackWallAutofocus` (lines 360–415).
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\alcove\AlcoveVisionPipeline.java`
  - Core computer-vision solver (`solveChecker`, `detectBrightPlateauVerticalSeam`, `findOptimal16x9Rect`, `detectCheckerCorners`, `assignCheckerLattice`).
  - Key sections: `solveChecker` (lines 110–390), `detectBrightPlateauVerticalSeam` (lines 400–525), `findOptimal16x9Rect` (lines 585–665).
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\alcove_upgrade\src\com\hysd\hyscreen\alcove\PatternOverlayView.java`
  - Renders `MODE_CHECKER_POS`, `MODE_CHECKER_NEG`, `MODE_ORIGIN_TAG`, and `MODE_HUD_CONFIRM` at `1920x1080`.
- `C:\Users\ngantla\Backups\Wzatco-Yuva-Blaze\BACKUP_README.md` & `SHA256SUMS.txt`
  - Master documentation and SHA-256 manifest for the 4.48 GiB factory backup, plus `custom_upgrades\com.hysd.screen-turbo-alcove.apk`.
- `C:\Users\ngantla\.copilot\session-state\4c8ab7fc-bd1f-480c-99ab-4458bfed9b0e\files\projector_artifacts\extracted\Htp_Jetter\classes.dump.txt`
  - Decompiled bytecode of `Htp_Jetter.apk` (`FocusMonitorService` at line 1498194, `BootReceiver` at line 1495897, `AfUtils` at line 405ac0) used to verify exact boot and repoint triggers.
</important_files>

<next_steps>
Immediate next steps:
1. View lines `1498942..1499350` of `Htp_Jetter\classes.dump.txt` (`FocusMonitorService.onCreate`/`onStartCommand`/`updateSensorData` and `Htp_Splash` if relevant) to confirm the exact cold-boot power-on trigger path alongside the `20 ms` IMU `DataDriftChecker` repoint trigger.
2. Answer both of the user's questions concisely and accurately:
   - Whether it aligns automatically on bootup (and how `persist.sys.poweron.autofocus=1` / `Htp_Splash` / `FocusMonitorService` invokes our upgraded `VAF2S`).
   - Exactly what happens when they physically repoint/move the projector (`FocusMonitorService` polling IMU drift every `20 ms` via `DataDriftChecker`, waiting for stillness, then calling `AfUtils.startFocus()` -> our `VAF2S` running center-wall autofocus + 3-frame complementary checkerboard sidewall/obstacle fit in ~1.5–2 s—plus note any nuance such as tilt/keystone vs. 16:9 crop if repointed at a steep angle).
3. Offer to adjust boot/repoint behavior if desired, or resume the earlier deferred security/telemetry audit.
</next_steps>