package com.hysd.hyscreen;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.graphics.PixelFormat;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.os.SystemProperties;
import android.util.Log;
import android.view.Gravity;
import android.view.WindowManager;

import com.hysd.hyscreen.alcove.AlcoveVisionPipeline;
import com.hysd.hyscreen.alcove.PatternOverlayView;
import com.hysd.vafocus.HYSDVAF2;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Upgraded VAF2S service extending the preserved stock VAF2T implementation.
 *
 * - Intercepts obstacle avoidance, screen align, and auto-keystone triggers with a fast
 *   4-frame structured-light + sidewall/alcove crease + defocus + 16:9 auto-resize pipeline.
 * - Preserves 100% compatibility with factory calibration, ToF test modes, and pure motor AF
 *   by delegating non-alcove modes directly to super (VAF2T).
 * - Runtime escape hatch: `setprop persist.hysd.turbo_disable 1` reverts immediately to stock VAF2T.
 */
public class VAF2S extends VAF2T {
    private static final String TAG = "TurboAlcoveVAF2S";
    private static final int YUYV_FRAME_BYTES = AlcoveVisionPipeline.CAM_W * AlcoveVisionPipeline.CAM_H * 2;

    private static final int PHASE_IDLE = 0;
    private static final int PHASE_STAGE1_TRAP = 1;
    private static final int PHASE_STAGE3_POST_CROP_AF = 3;

    // Reusable capture buffers (eliminates ~4.2 MB allocation per run!)
    private final byte[] sharedYuyv = new byte[YUYV_FRAME_BYTES];
    private final int[] sharedYPos = new int[AlcoveVisionPipeline.CAM_N];
    private final int[] sharedYNeg = new int[AlcoveVisionPipeline.CAM_N];
    private final int[] sharedYOrigin = new int[AlcoveVisionPipeline.CAM_N];

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile int vafPhase = PHASE_IDLE;
    private volatile boolean pendingPostCropAf = false;
    private volatile boolean pendingForceAf = false;
    private volatile float currentThrowDistMm = 0f;
    private volatile boolean pendingDfdBracket = false;
    private volatile int[] priorValidBaseTrap = null;
    private volatile int[] lockedFinalInsets = null;
    private volatile Intent pendingRerunIntent = null;
    private volatile int lastStartId = 1;
    private volatile String savedExitType = "2";
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    /**
     * Accelerates stock VAF2T's hardcoded Java UI/IPC sleeps (delay(300), delay(100), delay(1500))
     * while our pipeline is active, shaving ~600-1600ms off Stage 1 and Stage 3 without touching
     * native C++ motor step timing (which uses usleep inside libfocusvahysd.so).
     */
    @Override
    public void delay(int ms) {
        if (running.get()) {
            if (ms >= 1000) {
                ms = 100;
            } else if (ms >= 250) {
                ms = 55;
            } else if (ms >= 80) {
                ms = 22;
            }
        }
        super.delay(ms);
    }

    @Override
    public int onStartCommand(final Intent intent, int flags, int startId) {
        if ("1".equals(SystemProperties.get("persist.hysd.turbo_disable", "0"))) {
            Log.i(TAG, "persist.hysd.turbo_disable=1 -> delegating to stock VAF2T");
            return super.onStartCommand(intent, flags, startId);
        }

        if (intent == null) {
            return START_NOT_STICKY;
        }
        this.lastStartId = startId;

        final int mode = intent.getIntExtra("mode", 1);
        final int dmodeProp = SystemProperties.getInt("persist.htp.hysd.dmode", 3);
        final int dmode = intent.getIntExtra("dmode", dmodeProp);
        final int afSwitch = intent.getIntExtra("afswitch", SystemProperties.getInt("persist.sys.autofocus", 0));
        final int keySwitch = intent.getIntExtra("keyswitch", SystemProperties.getInt("persist.sys.autotrapezoid", 1));
        final boolean obsOpen = "1".equals(SystemProperties.get("persist.hysd.obs_open", "1"));
        final boolean mbOpen = "1".equals(SystemProperties.get("persist.hysd.mb_open", "1"));

        Log.i(TAG, "onStartCommand mode=" + mode + " dmode=" + dmode + " af=" + afSwitch + " key=" + keySwitch);

        // Instant <20ms keystone + base_trap reset for mode 5 / 6
        if (mode == 5 || mode == 6) {
            pendingRerunIntent = null;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    resetKeystoneFast();
                }
            }, "TurboKeyReset").start();
            return START_NOT_STICKY;
        }

        final boolean wantAlcoveAvoidance =
                (mode == 3 || mode == 7 || mode == 99)
                || ((mode == 1 || mode == 4 || mode == 20 || mode == 21) && (dmode > 0 || obsOpen || mbOpen));

        if (!wantAlcoveAvoidance) {
            Log.i(TAG, "Delegating non-obstacle mode=" + mode + " directly to stock VAF2T");
            return super.onStartCommand(intent, flags, startId);
        }

        if (!running.compareAndSet(false, true)) {
            // Queue latest repoint trigger so if the user nudges the projector mid-scan,
            // one clean trailing pass runs immediately once settled!
            pendingRerunIntent = new Intent(intent);
            Log.i(TAG, "Pipeline busy; queued trailing rerun for final projector pose.");
            return START_NOT_STICKY;
        }

        pendingRerunIntent = null;
        return dispatchPipelineFromMain(intent, flags, startId);
    }

    private int dispatchPipelineFromMain(final Intent intent, final int flags, final int startId) {
        final int mode = intent.getIntExtra("mode", 1);
        final int afSwitch = intent.getIntExtra("afswitch", SystemProperties.getInt("persist.sys.autofocus", 0));
        final int keySwitch = intent.getIntExtra("keyswitch", SystemProperties.getInt("persist.sys.autotrapezoid", 1));
        final boolean runDfdMotorBracket =
                "1".equals(SystemProperties.get("persist.hysd.alcove.dfd", "0"))
                || intent.getIntExtra("dfd", 0) == 1;

        final int[] cachedBaseTrap = parseBaseTrap(SystemProperties.get("persist.hysd.base_trap", ""));
        this.priorValidBaseTrap = cachedBaseTrap;
        this.pendingDfdBracket = runDfdMotorBracket;

        final boolean needFreshTrapezoid = (mode == 1 || mode == 4 || mode == 20 || mode == 21)
                || (keySwitch == 1 && cachedBaseTrap == null);
        this.pendingForceAf = intent.getIntExtra("force_af", 0) == 1
                || (mode == 1 && keySwitch == 0 && afSwitch == 1);
        this.pendingPostCropAf = (afSwitch == 1 && mode != 3 && mode != 7) || this.pendingForceAf;
        this.currentThrowDistMm = 0f;

        this.savedExitType = SystemProperties.get("persist.hysd.exittype", "2");
        if ("0".equals(this.savedExitType)) {
            this.savedExitType = "2";
        }

        if (!needFreshTrapezoid) {
            // Fast ~1.0s Stage-2-only path (e.g. manual Obstacle Avoidance button when trapezoid already known)
            new Thread(new Runnable() {
                @Override
                public void run() {
                    executeTurboAlcovePipeline(false, cachedBaseTrap, runDfdMotorBracket, 1);
                }
            }, "TurboAlcoveStage2Only").start();
            return START_NOT_STICKY;
        }

        // THREE-PHASE SNAPPY PIPELINE:
        //   Phase 1 (~1.2s): Fast 3D stereo Tsai/IMU trapezoid ONLY (afswitch=0, dmode=0 -> ZERO slow uncropped motor hunt!)
        //   Phase 2 (~0.95s): Constant-flux RGB(255) checkerboard Alcove 16:9 fit composed onto Phase 1 trapezoid
        //   Phase 3 (~2.2s, if af=1): Factory precision AF strictly INSIDE the final cropped 16:9 target screen!
        this.vafPhase = PHASE_STAGE1_TRAP;

        Intent stage1Intent = new Intent(intent);
        stage1Intent.putExtra("mode", 1);
        stage1Intent.putExtra("afswitch", 0); // Defer focus until AFTER sidewall crop!
        stage1Intent.putExtra("keyswitch", 1);
        stage1Intent.putExtra("dmode", 0);

        String origObs = SystemProperties.get("persist.hysd.obs_open", "1");
        String origMb = SystemProperties.get("persist.hysd.mb_open", "1");
        SystemProperties.set("persist.hysd.obs_open", "0");
        SystemProperties.set("persist.hysd.mb_open", "0");
        SystemProperties.set("persist.hysd.exittype", "0");

        final long watchdogToken = System.nanoTime();
        startPhaseWatchdog(watchdogToken, 14000);

        try {
            Log.i(TAG, "Phase 1 starting: fast 3D Tsai/IMU trapezoid (af=0, dmode=0); postCropAf=" + pendingPostCropAf);
            return super.onStartCommand(stage1Intent, flags, startId);
        } catch (Throwable t) {
            Log.e(TAG, "Phase 1 launch failed; aborting cleanly", t);
            vafPhase = PHASE_IDLE;
            SystemProperties.set("persist.hysd.exittype", savedExitType);
            finishOrRerun();
            return START_NOT_STICKY;
        } finally {
            SystemProperties.set("persist.hysd.obs_open", origObs);
            SystemProperties.set("persist.hysd.mb_open", origMb);
        }
    }

    private volatile long activeWatchdogToken = 0L;

    private void startPhaseWatchdog(final long token, final long timeoutMs) {
        this.activeWatchdogToken = token;
        new Thread(new Runnable() {
            @Override
            public void run() {
                sleepQuiet(timeoutMs);
                if (activeWatchdogToken == token && vafPhase != PHASE_IDLE) {
                    Log.w(TAG, "Phase watchdog expired after " + timeoutMs + "ms; restoring state.");
                    vafPhase = PHASE_IDLE;
                    SystemProperties.set("persist.hysd.exittype", savedExitType);
                    finishOrRerun();
                }
            }
        }, "TurboPhaseWatchdog").start();
    }

    @Override
    public int workfinished() {
        final int phase = this.vafPhase;
        if (phase != PHASE_IDLE) {
            SystemProperties.set("persist.hysd.exittype", "0");
        }
        int rc = super.workfinished();
        if (phase == PHASE_IDLE) {
            return rc;
        }

        if (phase == PHASE_STAGE1_TRAP) {
            this.vafPhase = PHASE_IDLE;
            // CRITICAL: Stop Phase 1's idle HYSDkeystone$motorbegin thread so it never races Phase 3!
            try {
                motor_stop();
                motorthreadstop(-3);
            } catch (Throwable ignored) {}

            // Extract stereo-triangulated throw distance (mm) from Phase 1's result_angle[9]!
            float measuredDistMm = 0f;
            if (this.result_angle != null && this.result_angle.length > 9 && this.result_angle[9] > 300f) {
                measuredDistMm = this.result_angle[9];
            }
            this.currentThrowDistMm = measuredDistMm;

            // Smart distance-gated AF: always focus on cold boot (lastDist==0), explicit force_af,
            // or when throw distance changed by > 4.5%; skip redundant 4s motor sweep on in-place pan/tilt!
            if (this.pendingPostCropAf && !this.pendingForceAf && measuredDistMm > 300f) {
                float lastAfDist = parseFloatSafe(SystemProperties.get("sys.hysd.last_af_dist_mm", "0"), 0f);
                if (lastAfDist > 300f) {
                    float relChange = Math.abs(measuredDistMm - lastAfDist) / lastAfDist;
                    if (relChange <= 0.045f) {
                        Log.i(TAG, String.format(
                                "Throw distance unchanged (%.0fmm vs %.0fmm, %.1f%% <= 4.5%%) -> skipping redundant motor sweep!",
                                measuredDistMm, lastAfDist, relChange * 100f));
                        this.pendingPostCropAf = false;
                    }
                }
            }

            SystemProperties.set("com.hysd.focusing", "1");
            Log.i(TAG, "Phase 1 trapezoid complete (rc=" + rc + ", distMm=" + measuredDistMm + ") -> chaining Phase 2 fast Alcove fit!");
            sleepQuiet(15);
            executeTurboAlcovePipeline(true, priorValidBaseTrap, pendingDfdBracket, rc);
            return rc;
        }

        if (phase == PHASE_STAGE3_POST_CROP_AF) {
            this.vafPhase = PHASE_IDLE;
            try {
                motor_stop();
                motorthreadstop(-3);
            } catch (Throwable ignored) {}
            if (rc >= 0 && this.currentThrowDistMm > 300f) {
                SystemProperties.set("sys.hysd.last_af_dist_mm", String.valueOf(Math.round(this.currentThrowDistMm)));
            }
            SystemProperties.set("persist.hysd.exittype", savedExitType);
            // Guarantee finalInsets was untouched by post-crop AF
            final int[] targetInsets = this.lockedFinalInsets;
            if (targetInsets != null && targetInsets.length == 8) {
                ServiceHolder aidl = bindHysdClient(800);
                try {
                    if (aidl.service != null) {
                        int[] cur = aidl.service.getkey();
                        if (!Arrays.equals(cur, targetInsets)) {
                            aidl.service.setkey(targetInsets[0], targetInsets[1], targetInsets[2], targetInsets[3],
                                    targetInsets[4], targetInsets[5], targetInsets[6], targetInsets[7]);
                        }
                        aidl.service.finishkey(1, 1, 0);
                    }
                } catch (Throwable ignored) {
                } finally {
                    aidl.unbind(this);
                }
            }
            SystemProperties.set("com.hysd.focusing", "0");
            Log.i(TAG, "Phase 3 post-crop target-wall autofocus complete (rc=" + rc + ")!");
            finishOrRerun();
            return rc;
        }

        return rc;
    }

    private void launchStage3PostCropAutofocus() {
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                try {
                    vafPhase = PHASE_STAGE3_POST_CROP_AF;
                    Intent afIntent = new Intent();
                    afIntent.putExtra("mode", 1);
                    afIntent.putExtra("afswitch", 1);
                    afIntent.putExtra("keyswitch", 0); // Keep finalInsets active on DisplayEngine!
                    afIntent.putExtra("dmode", 0);

                    String origObs = SystemProperties.get("persist.hysd.obs_open", "1");
                    String origMb = SystemProperties.get("persist.hysd.mb_open", "1");
                    SystemProperties.set("persist.hysd.obs_open", "0");
                    SystemProperties.set("persist.hysd.mb_open", "0");
                    SystemProperties.set("persist.hysd.exittype", "0");

                    final long token = System.nanoTime();
                    startPhaseWatchdog(token, 14000);
                    try {
                        Log.i(TAG, "Phase 3 starting: precision factory AF inside cropped 16:9 target wall!");
                        VAF2S.super.onStartCommand(afIntent, 0, lastStartId);
                    } finally {
                        SystemProperties.set("persist.hysd.obs_open", origObs);
                        SystemProperties.set("persist.hysd.mb_open", origMb);
                    }
                } catch (Throwable t) {
                    Log.e(TAG, "Phase 3 AF launch failed", t);
                    vafPhase = PHASE_IDLE;
                    SystemProperties.set("persist.hysd.exittype", savedExitType);
                    SystemProperties.set("com.hysd.focusing", "0");
                    finishOrRerun();
                }
            }
        });
    }

    private void finishOrRerun() {
        activeWatchdogToken = 0L;
        final Intent rerun = this.pendingRerunIntent;
        this.pendingRerunIntent = null;
        if (rerun != null) {
            Log.i(TAG, "Executing queued trailing rerun for final projector position!");
            mainHandler.post(new Runnable() {
                @Override
                public void run() {
                    dispatchPipelineFromMain(rerun, 0, lastStartId);
                }
            });
            return;
        }
        SystemProperties.set("persist.hysd.exittype", savedExitType);
        SystemProperties.set("com.hysd.focusing", "0");
        running.set(false);
        stopSelf();
    }

    private void resetKeystoneFast() {
        SystemProperties.set("persist.hysd.base_trap", "");
        SystemProperties.set("sys.hysd.last_af_dist_mm", "0");
        ServiceHolder holder = bindHysdClient(1500);
        try {
            if (holder.service != null) {
                holder.service.setkey(0, 0, 0, 0, 0, 0, 0, 0);
                holder.service.finishkey(1, 1, 0);
            }
        } catch (Throwable t) {
            Log.e(TAG, "Fast resetkey failed", t);
        } finally {
            holder.unbind(this);
            stopSelf();
        }
    }

    private static String formatBaseTrap(int[] q) {
        if (q == null || q.length != 8) return "";
        return q[0] + "," + q[1] + "," + q[2] + "," + q[3] + ","
                + q[4] + "," + q[5] + "," + q[6] + "," + q[7];
    }

    private static int[] parseBaseTrap(String s) {
        if (s == null || s.isEmpty()) return null;
        String[] parts = s.split(",");
        if (parts.length != 8) return null;
        int[] q = new int[8];
        try {
            for (int i = 0; i < 8; i++) q[i] = Integer.parseInt(parts[i].trim());
            return q;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static float parseFloatSafe(String s, float defVal) {
        if (s == null || s.isEmpty()) return defVal;
        try {
            float v = Float.parseFloat(s.trim());
            return (Float.isNaN(v) || Float.isInfinite(v)) ? defVal : v;
        } catch (NumberFormatException e) {
            return defVal;
        }
    }

    private void executeTurboAlcovePipeline(
            boolean readFreshStage1Trap, int[] fallbackBaseTrap, boolean runDfdBracket, int stage1Rc) {
        final long tStart = System.nanoTime();
        SystemProperties.set("com.hysd.focusing", "1");
        SystemProperties.set("keystatus.hysd.sf", "0");

        // Overlap V4L2 camera open (USB UVC stream-on) concurrently with AIDL bind + overlay attach!
        final int camId = SystemProperties.getInt("persist.keystone.camid", 0);
        final AtomicBoolean camReady = new AtomicBoolean(false);
        final CountDownLatch camLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    int rc = HYSDVAF2.s0cam(camId, 2);
                    if (rc != 1) {
                        sleepQuiet(35);
                        HYSDVAF2.s0cam(camId, 2);
                    }
                    camReady.set(true);
                } catch (Throwable t) {
                    Log.e(TAG, "Parallel s0cam error", t);
                } finally {
                    camLatch.countDown();
                }
            }
        }, "TurboCamOpen").start();

        final WindowManager wm = (WindowManager) getSystemService(Context.WINDOW_SERVICE);
        final PatternOverlayView[] asyncOverlayBox = new PatternOverlayView[1];
        final CountDownLatch overlayLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    asyncOverlayBox[0] = attachOverlay(wm);
                } finally {
                    overlayLatch.countDown();
                }
            }
        }, "TurboOverlayAttach").start();

        ServiceHolder aidl = bindHysdClient(1500);
        PatternOverlayView overlay = null;
        int[] baseTrap = null;
        boolean camOpened = false;
        Thread camCloseThread = null;
        Runnable deferredDebugSave = null;

        try {
            if (aidl.service == null) {
                Log.e(TAG, "Could not bind com.hysd.client.VAFclientapi");
                overlayLatch.await(500, TimeUnit.MILLISECONDS);
                overlay = asyncOverlayBox[0];
                camLatch.await(1200, TimeUnit.MILLISECONDS);
                camOpened = camReady.get();
                return;
            }

            if (readFreshStage1Trap) {
                int[] fresh = aidl.service.getkey();
                if (stage1Rc >= 0 && fresh != null && fresh.length == 8) {
                    baseTrap = fresh;
                    SystemProperties.set("persist.hysd.base_trap", formatBaseTrap(baseTrap));
                    Log.i(TAG, "Phase 2 active 3D baseTrap=" + Arrays.toString(baseTrap));
                } else if (fallbackBaseTrap != null && fallbackBaseTrap.length == 8) {
                    Log.w(TAG, "Phase 1 returned rc=" + stage1Rc + "; preserving prior valid baseTrap=" + Arrays.toString(fallbackBaseTrap));
                    baseTrap = fallbackBaseTrap.clone();
                    aidl.service.setkey(baseTrap[0], baseTrap[1], baseTrap[2], baseTrap[3],
                            baseTrap[4], baseTrap[5], baseTrap[6], baseTrap[7]);
                } else {
                    baseTrap = (fresh != null && fresh.length == 8) ? fresh : new int[8];
                }
            } else if (fallbackBaseTrap != null && fallbackBaseTrap.length == 8) {
                baseTrap = fallbackBaseTrap.clone();
                aidl.service.setkey(baseTrap[0], baseTrap[1], baseTrap[2], baseTrap[3],
                        baseTrap[4], baseTrap[5], baseTrap[6], baseTrap[7]);
            } else {
                baseTrap = new int[8];
                aidl.service.setkey(0, 0, 0, 0, 0, 0, 0, 0);
            }

            overlayLatch.await(1000, TimeUnit.MILLISECONDS);
            overlay = asyncOverlayBox[0];
            if (overlay == null) {
                Log.e(TAG, "Could not attach full-screen PatternOverlayView");
                camLatch.await(1200, TimeUnit.MILLISECONDS);
                camOpened = camReady.get();
                return;
            }
            overlay.setPatternAndAwaitDraw(PatternOverlayView.MODE_CHECKER_POS, 200);
            sleepQuiet(20);

            camLatch.await(2000, TimeUnit.MILLISECONDS);
            camOpened = camReady.get();

            final byte[] yuyv = this.sharedYuyv;
            final int[] yPos = this.sharedYPos;
            final int[] yNeg = this.sharedYNeg;
            final int[] yOrigin = this.sharedYOrigin;
            int[] yNearPos = null;

            // 1. Capture settled CHECKER_POS
            captureSettledCheckerBase(yuyv, yPos);

            // 2. Switch to exact pixelwise complement CHECKER_NEG (50% flux -> zero AE shift!)
            overlay.setPatternAndAwaitDraw(PatternOverlayView.MODE_CHECKER_NEG, 120);
            sleepQuiet(15);
            captureFlippedCheckerFrame(yuyv, yPos, yNeg);

            // 3. Switch to ORIGIN_TAG
            overlay.setPatternAndAwaitDraw(PatternOverlayView.MODE_ORIGIN_TAG, 120);
            sleepQuiet(15);
            captureFlippedCheckerFrame(yuyv, yNeg, yOrigin);

            if (runDfdBracket) {
                yNearPos = new int[AlcoveVisionPipeline.CAM_N];
                aidl.service.motorf();
                sleepQuiet(75);
                aidl.service.motors();
                sleepQuiet(25);
                captureFreshV4l2Frame(yuyv, yNearPos, 3);
                aidl.service.motorb();
                sleepQuiet(80);
                aidl.service.motors();
                sleepQuiet(15);
            }

            // Release V4L2 camera concurrently with CPU solve, then join before Phase 3 AF!
            camOpened = false;
            camCloseThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try { HYSDVAF2.s2cam(); } catch (Throwable ignored) {}
                }
            }, "TurboCamClose");
            camCloseThread.start();

            long captureMs = (System.nanoTime() - tStart) / 1_000_000L;
            float minArea = clampFloatSafe(
                    parseFloatSafe(SystemProperties.get("persist.hysd.alcove.min_area", "0.24"), 0.24f),
                    0.10f, 0.90f);
            boolean saveDebug = !"0".equals(SystemProperties.get("persist.hysd.alcove.debug", "0"));

            AlcoveVisionPipeline.Result res = AlcoveVisionPipeline.solveChecker(
                    yPos, yNeg, yOrigin, yNearPos, baseTrap, minArea, saveDebug);
            deferredDebugSave = res.deferredDebugSaver;

            long totalMs = (System.nanoTime() - tStart) / 1_000_000L;
            if (res.success) {
                int[] k = res.insets;
                this.lockedFinalInsets = k.clone();
                aidl.service.setkey(k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7]);
                aidl.service.finishkey(1, 1, 0);
                SystemProperties.set("keystatus.hysd.sf", "1");
                SystemProperties.set("keystatus.hysd.keyresult", "1");

                String hud = String.format("Alcove Fit %dx%d (%dms)",
                        (res.rectRightPx - res.rectLeftPx),
                        (res.rectBottomPx - res.rectTopPx),
                        totalMs);
                Log.i(TAG, "SUCCESS stage2=" + totalMs + "ms (capture=" + captureMs + "ms, solve=" + res.solveTimeMs
                        + "ms) baseTrap=" + Arrays.toString(baseTrap) + " finalInsets=" + Arrays.toString(k));

                if (!"0".equals(SystemProperties.get("persist.hysd.alcove.hud", "1")) && !pendingPostCropAf) {
                    overlay.showConfirmHud(k, hud);
                    sleepQuiet(80);
                }
            } else {
                Log.w(TAG, "Alcove solver did not converge (" + res.summary + "); preserving 3D base trapezoid.");
                if (baseTrap != null && baseTrap.length == 8) {
                    this.lockedFinalInsets = baseTrap.clone();
                    aidl.service.setkey(baseTrap[0], baseTrap[1], baseTrap[2], baseTrap[3],
                            baseTrap[4], baseTrap[5], baseTrap[6], baseTrap[7]);
                }
                aidl.service.finishkey(1, 0, -1);
            }
        } catch (Throwable t) {
            Log.e(TAG, "TurboAlcove pipeline error", t);
            try {
                if (baseTrap != null && baseTrap.length == 8 && aidl.service != null) {
                    this.lockedFinalInsets = baseTrap.clone();
                    aidl.service.setkey(baseTrap[0], baseTrap[1], baseTrap[2], baseTrap[3],
                            baseTrap[4], baseTrap[5], baseTrap[6], baseTrap[7]);
                }
            } catch (Throwable ignored) {}
        } finally {
            if (camOpened) {
                try { HYSDVAF2.s2cam(); } catch (Throwable ignored) {}
            }
            if (camCloseThread != null) {
                try { camCloseThread.join(600); } catch (InterruptedException ignored) {}
            }
            detachOverlay(wm, overlay);
            aidl.unbind(this);
            if (deferredDebugSave != null) {
                try { deferredDebugSave.run(); } catch (Throwable ignored) {}
            }
            if (pendingPostCropAf && pendingRerunIntent == null) {
                pendingPostCropAf = false;
                launchStage3PostCropAutofocus();
            } else {
                pendingPostCropAf = false;
                finishOrRerun();
            }
        }
    }

    private static float clampFloatSafe(float v, float lo, float hi) {
        return v < lo ? lo : (v > hi ? hi : v);
    }

    /**
     * Flushes 2 initial V4L2 ring buffers and waits until CHECKER_POS spatial contrast (P90 - P10 >= 12 DN)
     * is visibly projected on the wall AND frame-to-frame luma has settled (< 2.3 DN).
     */
    private static void captureSettledCheckerBase(byte[] yuyv, int[] outY) {
        for (int f = 0; f < 2; f++) {
            HYSDVAF2.s1cam(yuyv);
        }
        int[] prevSample = new int[2000];
        int[] hist = new int[256];
        boolean hasPrev = false;
        for (int i = 0; i < 14; i++) {
            HYSDVAF2.s1cam(yuyv);
            Arrays.fill(hist, 0);
            double diff = 0;
            int k = 0;
            for (int v = 60; v < 400; v += 10) {
                int row = v * AlcoveVisionPipeline.CAM_W;
                for (int u = 60; u < 560; u += 12) {
                    int val = yuyv[(row + u) << 1] & 0xFF;
                    hist[val]++;
                    if (hasPrev) diff += Math.abs(val - prevSample[k]);
                    prevSample[k++] = val;
                }
            }
            int p10 = 0, p90 = 0, cum = 0;
            for (int b = 0; b < 256; b++) {
                cum += hist[b];
                if (cum <= k * 0.10) p10 = b;
                if (cum <= k * 0.90) p90 = b;
            }
            int contrast = p90 - p10;
            if (hasPrev && contrast >= 12 && (diff / Math.max(1, k)) < 2.3) {
                break;
            }
            if (contrast < 12) {
                sleepQuiet(12);
            }
            hasPrev = true;
        }
        AlcoveVisionPipeline.extractLuma(yuyv, outY);
    }

    /**
     * Fast closed-loop signed correlation-inversion detector:
     * Accepts immediately upon full rolling-shutter inversion (strong inversion across >= 48% of dark tiles
     * AND formerly-bright tiles dropped into lower 36% of span), or on the next settled frame!
     */
    private static void captureFlippedCheckerFrame(byte[] yuyv, int[] refFrame, int[] outY) {
        int[] hist = new int[256];
        int cnt = 0;
        for (int v = 60; v < 400; v += 10) {
            int row = v * AlcoveVisionPipeline.CAM_W;
            for (int u = 60; u < 560; u += 10) {
                hist[refFrame[row + u] & 0xFF]++;
                cnt++;
            }
        }
        int p15 = 0, p88 = 0, cum = 0;
        for (int b = 0; b < 256; b++) {
            cum += hist[b];
            if (cum <= cnt * 0.15) p15 = b;
            if (cum <= cnt * 0.88) p88 = b;
        }
        final int refSpan = Math.max(12, p88 - p15);
        final int highThresh = p15 + (refSpan * 62) / 100;
        final int lowThresh  = p15 + (refSpan * 38) / 100;
        final int midThresh  = p15 + (refSpan * 50) / 100;

        long sumU = 0, sumV = 0, cntC = 0;
        for (int v = 45; v < 425; v += 10) {
            int row = v * AlcoveVisionPipeline.CAM_W;
            for (int u = 45; u < 585; u += 10) {
                if (refFrame[row + u] >= highThresh) {
                    sumU += u; sumV += v; cntC++;
                }
            }
        }
        final int centU = cntC > 10 ? (int) (sumU / cntC) : 290;
        final int centV = cntC > 10 ? (int) (sumV / cntC) : 200;

        int[] prevSample = new int[3200];
        boolean hasPrev = false;
        boolean invertedOnce = false;
        for (int i = 0; i < 20; i++) {
            HYSDVAF2.s1cam(yuyv);
            long sumAtRefBright = 0;
            int nBright = 0, nDarkFlipped = 0;
            double settleDiff = 0;
            int sampleIdx = 0;

            for (int v = 45; v < 425; v += 10) {
                int row = v * AlcoveVisionPipeline.CAM_W;
                for (int u = 45; u < 585; u += 10) {
                    if (Math.abs(u - centU) <= 58 && Math.abs(v - centV) <= 46) continue;
                    int val = yuyv[(row + u) << 1] & 0xFF;
                    int refVal = refFrame[row + u];
                    if (hasPrev) {
                        settleDiff += Math.abs(val - prevSample[sampleIdx]);
                    }
                    prevSample[sampleIdx++] = val;

                    if (refVal >= highThresh) {
                        sumAtRefBright += val;
                        nBright++;
                    } else if (refVal <= lowThresh && val >= midThresh) {
                        nDarkFlipped++;
                    }
                }
            }
            double meanSettle = hasPrev ? (settleDiff / Math.max(1, sampleIdx)) : 99.0;
            hasPrev = true;

            double meanAtBright = nBright > 8 ? (sumAtRefBright / (double) nBright) : 255.0;
            boolean strongFullInversion = (nBright >= 12)
                    && (nDarkFlipped >= Math.max(12, (nBright * 48) / 100))
                    && (meanAtBright <= p15 + refSpan * 0.36);
            boolean trulyInverted = (nBright >= 12)
                    && (nDarkFlipped >= Math.max(10, nBright / 3))
                    && (meanAtBright <= p15 + refSpan * 0.45);

            if (strongFullInversion || (trulyInverted && invertedOnce && meanSettle < 3.0)) {
                break;
            }
            if (trulyInverted) {
                invertedOnce = true;
            } else {
                sleepQuiet(12);
            }
        }
        AlcoveVisionPipeline.extractLuma(yuyv, outY);
    }

    /**
     * Explicitly flushes `discardCount` queued V4L2 mmap ring buffers and captures the next fresh frame.
     */
    private static void captureFreshV4l2Frame(byte[] yuyv, int[] outY, int discardCount) {
        for (int i = 0; i < discardCount; i++) {
            HYSDVAF2.s1cam(yuyv);
        }
        int rc = HYSDVAF2.s1cam(yuyv);
        if (rc < 0) {
            sleepQuiet(20);
            HYSDVAF2.s1cam(yuyv);
        }
        if (outY != null) {
            AlcoveVisionPipeline.extractLuma(yuyv, outY);
        }
    }

    private PatternOverlayView attachOverlay(final WindowManager wm) {
        if (wm == null) return null;
        final PatternOverlayView[] box = new PatternOverlayView[1];
        final CountDownLatch latch = new CountDownLatch(1);
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                try {
                    PatternOverlayView v = new PatternOverlayView(VAF2S.this);
                    int type = Build.VERSION.SDK_INT >= 26
                            ? WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
                            : WindowManager.LayoutParams.TYPE_SYSTEM_ALERT;
                    // Omit FLAG_NOT_TOUCHABLE so Android 12+ WindowManager keeps alpha=1.00 (no 80% dimming!)
                    int flags = WindowManager.LayoutParams.FLAG_FULLSCREEN
                            | WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN
                            | WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS
                            | WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE
                            | WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON
                            | WindowManager.LayoutParams.FLAG_HARDWARE_ACCELERATED;
                    WindowManager.LayoutParams lp = new WindowManager.LayoutParams(
                            PatternOverlayView.PROJ_W,
                            PatternOverlayView.PROJ_H,
                            type, flags, PixelFormat.OPAQUE);
                    lp.gravity = Gravity.TOP | Gravity.LEFT;
                    if (Build.VERSION.SDK_INT >= 28) {
                        lp.layoutInDisplayCutoutMode =
                                WindowManager.LayoutParams.LAYOUT_IN_DISPLAY_CUTOUT_MODE_SHORT_EDGES;
                    }
                    wm.addView(v, lp);
                    box[0] = v;
                } catch (Throwable t) {
                    Log.e(TAG, "Failed adding overlay window", t);
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            latch.await(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        return box[0];
    }

    private void detachOverlay(final WindowManager wm, final PatternOverlayView view) {
        if (wm == null || view == null) return;
        final CountDownLatch latch = new CountDownLatch(1);
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                try {
                    wm.removeViewImmediate(view);
                } catch (Throwable ignored) {
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            latch.await(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private ServiceHolder bindHysdClient(long timeoutMs) {
        final ServiceHolder holder = new ServiceHolder();
        String pkg = SystemProperties.get("persist.hysd.aidl.package", "com.hysd.client");
        String cls = SystemProperties.get("persist.hysd.aidl.class", "com.hysd.client.VAFclientapi");
        Intent i = new Intent();
        i.setAction(pkg);
        i.addCategory(Intent.CATEGORY_DEFAULT);
        i.setComponent(new ComponentName(pkg, cls));
        holder.bound = bindService(i, holder.conn, Context.BIND_AUTO_CREATE);
        if (holder.bound) {
            try {
                holder.latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        return holder;
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class ServiceHolder {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile HYSDInterface service;
        volatile boolean bound;

        final ServiceConnection conn = new ServiceConnection() {
            @Override
            public void onServiceConnected(ComponentName name, IBinder binder) {
                service = HYSDInterface.Stub.asInterface(binder);
                latch.countDown();
            }

            @Override
            public void onServiceDisconnected(ComponentName name) {
                service = null;
            }
        };

        void unbind(Context ctx) {
            if (bound) {
                try { ctx.unbindService(conn); } catch (Throwable ignored) {}
                bound = false;
            }
        }
    }
}
