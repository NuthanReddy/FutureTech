# Plan: Documenting Faster On-Device Object Avoidance & Auto-Resize

## Problem Statement & Proposed Approach

The WZATCO Yuva Blaze (`Allwinner H727`, 32-bit `armeabi-v7a`, 4× Cortex-A53, 977 MB total RAM / ~169 MB available RAM) currently performs obstacle avoidance and screen-fit through `com.hysd.hyscreen` (`VAF2S` + `libfocusvahysd.so`). Reverse engineering (`ev_74be7926…`, `ev_380c264c…`, `ev_0cf5c441…`, `ev_7ac16f73…`) shows that the stock implementation relies on heavy multi-stage UI dialogs, fixed thread sleeps, single-frame JPEG decoding, and OpenCV thresholding/K-Means contour heuristics at `640×480` that confuse ambient shadows or wall texture with physical obstacles—and shrink the 1080p frame aggressively without enforcing a sharpness/pixel-retention floor (leaving the device at `(320, 160)` corner insets, discarding over half of native pixels).

We will author a comprehensive, implementation-ready technical design document in this session's Library (`files/object-avoidance-improvements.md`) paired with focused Mermaid architecture and algorithm diagrams (`files/current-vs-proposed-pipeline.mmd` and `files/on-device-integration-architecture.mmd`), detailing how to build and deploy a faster, sharper, memory-safe **on-device** object detection and auto-resize pipeline on the projector without reflashing `/system`.

## Deliverables

- `files/object-avoidance-improvements.md`: Main technical design specification in the session Library.
- `files/current-vs-proposed-pipeline.mmd`: Readable Mermaid diagram comparing the stock `VAF2S`/`libfocusvahysd.so` flow against the proposed differential + NCNN INT8 + sharpness-aware 16:9 solver pipeline.
- `files/on-device-integration-architecture.mmd`: Readable Mermaid diagram showing Android 14 on-device integration across `/dev/video0`, `/sdcard/focus` Tsai calibration data, ARM NEON / NCNN worker, and the exported `com.hysd.client.VAFclientapi` (`HYSDInterface.setkey`) hardware keystone bridge.

## Todos

- **doc-baseline-constraints**: Document verified hardware constraints, reverse-engineered stock `HYSD` pipeline (`VAF2S`, `HYCAM`, `CTsai`, `sfk`), REA evidence citations, and root causes of slow execution and aggressive downscale blur.
- **doc-fast-detection-pipeline**: Specify the faster on-device detection design: zero-JPEG YUYV/luma V4L2 capture on `/dev/video0`, two-frame differential structured-light subtraction (`I_on - I_off`) using ARMv7 NEON SIMD to cancel ambient light/shadows, and lightweight quantized NCNN INT8 semantic obstacle refinement within a strict low-RAM budget.
- **doc-sharpness-aware-autoresize**: Define the Tsai homography coordinate mapping (reusing factory `/sdcard/focus/{camdata,hybin}`), the $O(W \times H)$ histogram dynamic-programming solver for the largest unobstructed 16:9 rectangle, and the sharpness-preserving objective function (minimum pixel-retention floor, aspect lock, and skew penalty, plus Allwinner `PQControl` SNR/SSR interplay).
- **doc-android-integration-diagrams**: Create the two standalone Mermaid diagrams (12–15 nodes each) and document concrete on-device deployment mechanics (user-space APK binding exported `com.hysd.client.VAFclientapi` AIDL `setkey()`, UVC/V4L2 camera contention handling with `com.hysd.hyscreen`, SELinux/USB fallback, and rollback/reset safety).
- **publish-design-deliverables**: Review all generated artifacts for accuracy, REA evidence provenance, and diagram clarity, then publish the Markdown design document and diagrams to the Library preview panel via `publish_chat_update`.

## Notes & Considerations

- **Strict Hardware Budget**: All on-device data structures, camera ring buffers, and optional NCNN INT8 weights must fit comfortably inside ~169 MB available RAM on 32-bit `armeabi-v7a` (`armv7l` NEON) without triggering Android's Low Memory Killer (`lmkd`).
- **Non-Destructive Deployment**: Because `com.hysd.client.VAFclientapi` is exported without a signature permission (`exported="true"`), a standard sideloaded APK can drive hardware keystone (`HYSDInterface.setkey`) and focus motor APIs without modifying `/system` or risking a brick.
- **Camera & SELinux Boundary**: Although `/dev/video0` has `0666` permissions and `config.disable_cameraservice=true` disables Android's Camera2 service, Android 14 SELinux (`untrusted_app` vs `video_device`) may block direct `open("/dev/video0")` from a normal APK unless accessed via Android `UsbManager` (UVC host device) or an ADB `shell`-launched helper daemon; the document will cover both clean paths explicitly.
- **Evidence Provenance**: Distinguish verified static/ADB observations (with REA `ev_*` IDs) from un-decompiled native internals inside `libfocusvahysd.so`.
