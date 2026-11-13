package com.hysd.hyscreen.alcove;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Multi-cue fast on-device alcove sidewall + obstacle avoidance solver.
 *
 * Combines 4 physical signals on 640x480 raw YUYV luma frames:
 *   1. Ambient-canceled irradiance map W = max(0, Y_white - Y_dark)
 *   2. 91-dot self-calibrating fiducial lattice + RANSAC planar homography H_back
 *      (detects 3D depth steps & non-coplanar side-wall slope kinks via parallax residual)
 *   3. Photometric straight-line corner crease detection (Lambertian cos(theta) normal step
 *      at alcove side walls, protruding pillars, and top soffits)
 *   4. High-frequency vertical grating contrast S(x,y) (Depth-from-Defocus + oblique grazing blur,
 *      with optional 2-step focus-motor DfD ratio S_near / S_0)
 */
public final class AlcoveVisionPipeline {
    private static final String TAG = "TurboAlcove";

    public static final int CAM_W = 640;
    public static final int CAM_H = 480;
    public static final int CAM_N = CAM_W * CAM_H;

    // 192 x 108 working grid in projector space (1 cell = 10x10 projector pixels)
    public static final int GW = 192;
    public static final int GH = 108;
    public static final int CELL_PX = 10;

    public static final class Result {
        public final boolean success;
        public final int[] insets; // [ltx, lty, lbx, lby, rbx, rby, rtx, rty]
        public final int rectLeftPx;
        public final int rectTopPx;
        public final int rectRightPx;
        public final int rectBottomPx;
        public final int detectedLeftWallPx;
        public final int detectedRightWallPx;
        public final int detectedTopWallPx;
        public final int detectedBottomWallPx;
        public final int matchedDots;
        public final long solveTimeMs;
        public final String summary;
        public final Runnable deferredDebugSaver;

        public Result(
                boolean success, int[] insets,
                int l, int t, int r, int b,
                int lw, int rw, int tw, int bw,
                int matchedDots, long solveTimeMs, String summary, Runnable deferredDebugSaver) {
            this.success = success;
            this.insets = insets;
            this.rectLeftPx = l;
            this.rectTopPx = t;
            this.rectRightPx = r;
            this.rectBottomPx = b;
            this.detectedLeftWallPx = lw;
            this.detectedRightWallPx = rw;
            this.detectedTopWallPx = tw;
            this.detectedBottomWallPx = bw;
            this.matchedDots = matchedDots;
            this.solveTimeMs = solveTimeMs;
            this.summary = summary;
            this.deferredDebugSaver = deferredDebugSaver;
        }
    }

    private static final class CornerNode {
        float u, v;
        int parity; // +1 if even (c+r), -1 if odd (c+r)
        float strength;
        float originScore;
        int assignedCol = -1;
        int assignedRow = -1;
    }

    public static void extractLuma(byte[] yuyv, int[] outY) {
        final int n = Math.min(CAM_N, yuyv.length >> 1);
        for (int i = 0, j = 0; i < n; i++, j += 2) {
            outY[i] = yuyv[j] & 0xFF;
        }
    }

    /**
     * Fast Tenengrad focus metric on central 160x120 camera ROI (~0.08 ms) for closed-loop AF.
     */
    public static double computeCenterFocusScore(byte[] yuyv) {
        int u0 = (CAM_W - 160) / 2, u1 = u0 + 160;
        int v0 = (CAM_H - 120) / 2, v1 = v0 + 120;
        long sum = 0;
        for (int v = v0 + 1; v < v1 - 1; v++) {
            int row = v * CAM_W;
            for (int u = u0 + 1; u < u1 - 1; u++) {
                int gx = (yuyv[((row + u + 1) << 1)] & 0xFF) - (yuyv[((row + u - 1) << 1)] & 0xFF);
                int gy = (yuyv[((row + CAM_W + u) << 1)] & 0xFF) - (yuyv[((row - CAM_W + u) << 1)] & 0xFF);
                sum += gx * gx + gy * gy;
            }
        }
        return sum / (double) (158 * 118);
    }

    /**
     * Projectively composes a factory 3D perspective trapezoid quad `baseTrap`
     * ([ltx0, lty0, lbx0, lby0, rbx0, rby0, rtx0, rty0]) with a rectified 16:9 sub-rectangle
     * [lPx..rPx] x [tPx..bPx] in [0..1920] x [0..1080] wall space.
     *
     * Because PGL(3, R) is closed under composition, mapping the 4 sub-rectangle corners through
     * the unique 3x3 homography H_trap of `baseTrap` preserves exact 3D perspective vanishing points
     * so the cropped projection on the wall remains a dead-level, square-cornered 16:9 rectangle!
     */
    public static int[] composeTrapezoidAndCrop(int[] baseTrap, int lPx, int tPx, int rPx, int bPx) {
        final double W = PatternOverlayView.PROJ_W;
        final double H = PatternOverlayView.PROJ_H;

        boolean validBase = false;
        double x00 = 0, y00 = 0, x10 = W, y10 = 0, x11 = W, y11 = H, x01 = 0, y01 = H;
        if (baseTrap != null && baseTrap.length == 8) {
            double c00x = baseTrap[0], c00y = baseTrap[1];
            double c01x = baseTrap[2], c01y = H - baseTrap[3];
            double c11x = W - baseTrap[4], c11y = H - baseTrap[5];
            double c10x = W - baseTrap[6], c10y = baseTrap[7];
            if (c10x - c00x >= 420 && c11x - c01x >= 420
                    && c01y - c00y >= 240 && c11y - c10y >= 240
                    && c00x >= -2 && c00y >= -2 && c11x <= W + 2 && c11y <= H + 2) {
                x00 = c00x; y00 = c00y;
                x10 = c10x; y10 = c10y;
                x11 = c11x; y11 = c11y;
                x01 = c01x; y01 = c01y;
                validBase = true;
            }
        }

        // Fast exact pass-through if open wall with no sidewall/obstacle intrusion
        if (lPx <= 16 && tPx <= 16 && rPx >= PatternOverlayView.PROJ_W - 16 && bPx >= PatternOverlayView.PROJ_H - 16) {
            if (validBase) {
                return baseTrap.clone();
            }
            return new int[8];
        }

        double[] unitX = { 0.0, 1.0, 1.0, 0.0 };
        double[] unitY = { 0.0, 0.0, 1.0, 1.0 };
        double[] dstX  = { x00, x10, x11, x01 };
        double[] dstY  = { y00, y10, y11, y01 };

        double[] Htrap = HomographySolver.solveLeastSquares(unitX, unitY, dstX, dstY, 4);
        if (Htrap == null) {
            return new int[] {
                lPx, tPx,
                lPx, PatternOverlayView.PROJ_H - bPx,
                PatternOverlayView.PROJ_W - rPx, PatternOverlayView.PROJ_H - bPx,
                PatternOverlayView.PROJ_W - rPx, tPx
            };
        }

        double s0 = lPx / W, s1 = rPx / W;
        double t0 = tPx / H, t1 = bPx / H;

        double[] p00 = new double[2], p10 = new double[2], p11 = new double[2], p01 = new double[2];
        HomographySolver.project(Htrap, s0, t0, p00);
        HomographySolver.project(Htrap, s1, t0, p10);
        HomographySolver.project(Htrap, s1, t1, p11);
        HomographySolver.project(Htrap, s0, t1, p01);

        return new int[] {
            clamp((int) Math.round(p00[0]), 0, 1440),
            clamp((int) Math.round(p00[1]), 0, 810),
            clamp((int) Math.round(p01[0]), 0, 1440),
            clamp((int) Math.round(H - p01[1]), 0, 810),
            clamp((int) Math.round(W - p11[0]), 0, 1440),
            clamp((int) Math.round(H - p11[1]), 0, 810),
            clamp((int) Math.round(W - p10[0]), 0, 1440),
            clamp((int) Math.round(p10[1]), 0, 810)
        };
    }

    // Pre-allocated static working buffers (eliminates ~13 MB/scan heap churn & ART GC pauses on 1 GB RAM!)
    private static final Object SOLVE_LOCK = new Object();
    private static final int[] BUF_S = new int[CAM_N];
    private static final int[] BUF_ABS_S = new int[CAM_N];
    private static final int[] BUF_ORIG_DIFF = new int[CAM_N];
    private static final int[] BUF_W = new int[CAM_N];
    private static final int[] BUF_TMP_I = new int[CAM_N];
    private static final int[] BUF_SIGN = new int[CAM_N];
    private static final float[] BUF_RESP = new float[CAM_N];
    private static final float[] BUF_SHARP_RAW = new float[CAM_N];
    private static final float[] BUF_SHARP_TMP = new float[CAM_N];
    private static final float[] BUF_SHARP_OUT = new float[CAM_N];

    public static Result solveChecker(
            int[] yPos, int[] yNeg, int[] yOrigin,
            int[] yNearPosOptional, int[] baseTrapOptional, float minAreaRatio, boolean saveDebug) {
        synchronized (SOLVE_LOCK) {
            return solveCheckerLocked(yPos, yNeg, yOrigin, yNearPosOptional, baseTrapOptional, minAreaRatio, saveDebug);
        }
    }

    private static Result solveCheckerLocked(
            int[] yPos, int[] yNeg, int[] yOrigin,
            int[] yNearPosOptional, int[] baseTrapOptional, float minAreaRatio, boolean saveDebug) {
        final long t0 = System.nanoTime();

        // 1. Signed complementary field S = yPos - yNeg (zero AE shift, zero ambient/interreflection!)
        //    and seamless direct-beam radiance envelope W = maxFilter13x13(|S|) (bridges 3px checker seams!).
        final int[] S = BUF_S;
        final int[] absS = BUF_ABS_S;
        final int[] origDiff = BUF_ORIG_DIFF;
        for (int i = 0; i < CAM_N; i++) {
            int s = yPos[i] - yNeg[i];
            S[i] = s;
            absS[i] = s >= 0 ? s : -s;
            int od = yOrigin[i] - yPos[i];
            origDiff[i] = od >= 0 ? od : -od;
        }
        final int[] W = maxFilterSeparable(absS, 6, BUF_TMP_I, BUF_W);

        // Compute central beam amplitude percentile wP85
        int[] histW = new int[256];
        int centerSamples = 0;
        for (int v = CAM_H / 5; v < (CAM_H * 4) / 5; v++) {
            int off = v * CAM_W;
            for (int u = CAM_W / 6; u < (CAM_W * 5) / 6; u++) {
                int w = clamp(W[off + u], 0, 255);
                histW[w]++;
                centerSamples++;
            }
        }
        int targetRank = (int) (centerSamples * 0.85f);
        int cum = 0, wP85 = 50;
        for (int b = 0; b < 256; b++) {
            cum += histW[b];
            if (cum >= targetRank) { wP85 = b; break; }
        }
        final int beamThresh = Math.max(8, Math.round(wP85 * 0.22f));
        final int normFloor = Math.max(10, Math.round(wP85 * 0.25f));

        // 2. Detect 19x11 checkerboard saddle corners (c in 1..19, r in 1..11) + L-triad origin at (10,6)
        final int nCols = PatternOverlayView.TILE_COLS; // corners 1..19
        final int nRows = PatternOverlayView.TILE_ROWS; // corners 1..11
        List<CornerNode> corners = detectCheckerCorners(S, W, origDiff, beamThresh);
        int[][] gridToCorner = new int[nCols][nRows];
        for (int c = 0; c < nCols; c++) Arrays.fill(gridToCorner[c], -1);

        int matchedDots = assignCheckerLattice(corners, gridToCorner, wP85);
        Log.i(TAG, "CheckerCorners=" + corners.size() + " matched=" + matchedDots + " wP85=" + wP85 + " beamThresh=" + beamThresh);

        // 3. Fit main back-wall planar homography H_back and sanity-validate against outer beam bounds!
        double[] beamQuadH = fitBeamQuadFallbackHomography(W, beamThresh);
        double[] Hback = null;
        if (matchedDots >= 8) {
            double[] allX = new double[matchedDots];
            double[] allY = new double[matchedDots];
            double[] allU = new double[matchedDots];
            double[] allV = new double[matchedDots];
            boolean[] seedMask = new boolean[matchedDots];
            int k = 0;
            for (int c = 1; c < nCols; c++) {
                for (int r = 1; r < nRows; r++) {
                    int ci = gridToCorner[c][r];
                    if (ci < 0) continue;
                    CornerNode cn = corners.get(ci);
                    allX[k] = PatternOverlayView.cornerProjX(c);
                    allY[k] = PatternOverlayView.cornerProjY(r);
                    allU[k] = cn.u;
                    allV[k] = cn.v;
                    seedMask[k] = (c >= 5 && c <= 15 && r >= 3 && r <= 9);
                    k++;
                }
            }
            double[] candH = HomographySolver.fitRansac(allX, allY, allU, allV, seedMask, k, 2.2);
            if (isHomographyPlausible(candH)) {
                Hback = candH;
            } else {
                Log.w(TAG, "Rejected implausible lattice homography; using clean beam-quad homography.");
                matchedDots = 0;
            }
        }
        if (Hback == null) {
            Hback = beamQuadH;
        }
        if (Hback == null) {
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            return new Result(false, new int[8], 0, 0, 1920, 1080, 0, 1920, 0, 1080,
                    matchedDots, ms, "Homography failed", null);
        }

        // 4. Compute per-corner 3D homography parallax & sidewall foreshortening kink
        float[][] dotParallax = new float[nCols][nRows];
        boolean[][] dotObserved = new boolean[nCols][nRows];
        double[] uvPred = new double[2];
        double[] uvNextPred = new double[2];

        for (int c = 1; c < nCols; c++) {
            for (int r = 1; r < nRows; r++) {
                int ci = gridToCorner[c][r];
                if (ci < 0) continue;
                CornerNode cn = corners.get(ci);
                HomographySolver.project(Hback, PatternOverlayView.cornerProjX(c), PatternOverlayView.cornerProjY(r), uvPred);
                float err = (float) Math.hypot(cn.u - uvPred[0], cn.v - uvPred[1]);

                float kinkScore = 0f;
                if (c + 1 < nCols && gridToCorner[c + 1][r] >= 0) {
                    CornerNode next = corners.get(gridToCorner[c + 1][r]);
                    HomographySolver.project(Hback, PatternOverlayView.cornerProjX(c + 1), PatternOverlayView.cornerProjY(r), uvNextPred);
                    double obsDu = next.u - cn.u, obsDv = next.v - cn.v;
                    double expDu = uvNextPred[0] - uvPred[0], expDv = uvNextPred[1] - uvPred[1];
                    double obsLen = Math.hypot(obsDu, obsDv);
                    double expLen = Math.max(4.0, Math.hypot(expDu, expDv));
                    kinkScore = (float) Math.max(Math.hypot(obsDu - expDu, obsDv - expDv),
                            Math.abs(obsLen / expLen - 1.0) * 10.0);
                }
                dotParallax[c][r] = Math.max(err, kinkScore * 0.75f);
                dotObserved[c][r] = true;
            }
        }

        // 5. Checkerboard edge-spread sharpness map S0 (steepness of vertical tile transitions / W)
        float[] camSharp0 = computeCheckerEdgeSharpness(S, W, beamThresh, normFloor,
                BUF_SHARP_RAW, BUF_SHARP_TMP, BUF_SHARP_OUT);
        float[] camDfdRatio = null;
        if (yNearPosOptional != null) {
            int[] nearS = new int[CAM_N];
            for (int i = 0; i < CAM_N; i++) nearS[i] = yNearPosOptional[i] - yNeg[i];
            float[] camSharpNear = computeCheckerEdgeSharpness(nearS, W, beamThresh, normFloor,
                    new float[CAM_N], new float[CAM_N], new float[CAM_N]);
            camDfdRatio = new float[CAM_N];
            for (int i = 0; i < CAM_N; i++) {
                camDfdRatio[i] = (camSharp0[i] > 0.03f)
                        ? ((camSharpNear[i] + 0.02f) / (camSharp0[i] + 0.02f)) : 1.0f;
            }
        }

        // 6. Warp W, sharpness, DfD, and corner parallax onto 192x108 projector space
        float[][] projW = new float[GW][GH];
        float[][] projSharp = new float[GW][GH];
        float[][] projDfd = new float[GW][GH];
        float[][] projParallax = new float[GW][GH];
        boolean[][] inCameraBounds = new boolean[GW][GH];

        for (int gx = 0; gx < GW; gx++) {
            double px = (gx + 0.5) * CELL_PX;
            for (int gy = 0; gy < GH; gy++) {
                double py = (gy + 0.5) * CELL_PX;
                HomographySolver.project(Hback, px, py, uvPred);
                float u = (float) uvPred[0];
                float v = (float) uvPred[1];
                if (u >= 2f && u < CAM_W - 3f && v >= 2f && v < CAM_H - 3f) {
                    inCameraBounds[gx][gy] = true;
                    projW[gx][gy] = sampleBilinearInt(W, u, v);
                    projSharp[gx][gy] = sampleBilinearFloat(camSharp0, u, v);
                    projDfd[gx][gy] = (camDfdRatio != null) ? sampleBilinearFloat(camDfdRatio, u, v) : 1.0f;
                }
                projParallax[gx][gy] = interpolateCheckerParallax(dotParallax, dotObserved, (float) px, (float) py);
            }
        }

        // Normalize against central bright alcove plateau [gx in 50..125, gy in 20..85]
        float centerW = percentileGrid(projW, 50, 125, 20, 85, 0.65f);
        if (centerW < 15f) centerW = Math.max(20f, wP85);
        float[][] normW = new float[GW][GH];
        for (int gx = 0; gx < GW; gx++) {
            float dx = (gx - GW * 0.45f) / (GW * 0.55f);
            for (int gy = 0; gy < GH; gy++) {
                float dy = (gy - GH * 0.5f) / (GH * 0.5f);
                float vignette = Math.max(0.76f, 1.0f - 0.12f * (dx * dx + dy * dy));
                normW[gx][gy] = projW[gx][gy] / (centerW * vignette);
            }
        }

        float refSharp = percentileGrid(projSharp, 45, 130, 20, 85, 0.70f);

        // 7. Detect Left & Right Alcove Wall Seams (locking onto the bright central wall plateau!)
        int leftSeamGx = detectBrightPlateauVerticalSeam(normW, projW, projParallax, projSharp, projDfd,
                beamThresh, refSharp, true);
        int rightSeamGx = detectBrightPlateauVerticalSeam(normW, projW, projParallax, projSharp, projDfd,
                beamThresh, refSharp, false);
        int topSeamGy = detectHorizontalWallSeam(normW, projW, projParallax, projSharp, projDfd,
                beamThresh, refSharp, true, leftSeamGx, rightSeamGx);
        int bottomSeamGy = detectHorizontalWallSeam(normW, projW, projParallax, projSharp, projDfd,
                beamThresh, refSharp, false, leftSeamGx, rightSeamGx);

        // 8. Build Valid Central Bright-Wall Mask (excluding grey sidewalls & true obstacles!)
        float[][] smoothNormW = boxSmooth(normW, 6);
        float plateauRef = percentileGrid(normW, leftSeamGx + 3, rightSeamGx - 3, topSeamGy + 3, bottomSeamGy - 3, 0.55f);
        if (plateauRef < 0.5f) plateauRef = 0.85f;

        boolean[][] rawValid = new boolean[GW][GH];
        int interiorTotal = 0, interiorValid = 0;
        final float minBeamW = Math.max(5f, beamThresh * 0.85f);
        for (int gx = 0; gx < GW; gx++) {
            for (int gy = 0; gy < GH; gy++) {
                if (gx < leftSeamGx || gx > rightSeamGx || gy < topSeamGy || gy > bottomSeamGy) continue;
                boolean insideInterior = (gx >= leftSeamGx + 2 && gx <= rightSeamGx - 2
                        && gy >= topSeamGy + 2 && gy <= bottomSeamGy - 2);
                if (insideInterior) interiorTotal++;

                if (!inCameraBounds[gx][gy]) {
                    // Preserve outermost 2 cells along uncropped optical borders where Hback projects near sensor edge
                    boolean nearUncroppedBorder =
                            (leftSeamGx <= 2 && gx <= 2)
                            || (rightSeamGx >= GW - 3 && gx >= GW - 3)
                            || (topSeamGy <= 2 && gy <= 2)
                            || (bottomSeamGy >= GH - 3 && gy >= GH - 3);
                    if (!nearUncroppedBorder) continue;
                    rawValid[gx][gy] = true;
                    continue;
                }
                if (projW[gx][gy] < minBeamW) continue;

                float nw = normW[gx][gy];
                float localBg = Math.max(0.24f, smoothNormW[gx][gy]);
                // Preserve natural cos^4 corner vignetting at >3m while rejecting genuinely dark obstacles
                if (nw < plateauRef * 0.33f || nw < localBg * 0.58f) continue;

                if (camDfdRatio != null && projDfd[gx][gy] > 1.18f) continue;
                rawValid[gx][gy] = true;
                if (insideInterior) interiorValid++;
            }
        }

        // Fill isolated checkerboard-seam ripples / dust specks (< 3x3) and preserve non-seam outer borders
        // so border erosion never steals 40-180px on open sides!
        boolean[][] closed = fillSmallHoles5x5(
                morphClose2(rawValid, leftSeamGx, rightSeamGx, topSeamGy, bottomSeamGy),
                leftSeamGx, rightSeamGx, topSeamGy, bottomSeamGy);
        boolean[][] safeValid = erodePreservingOpenBorders(
                closed, 1, leftSeamGx, rightSeamGx, topSeamGy, bottomSeamGy);

        int lPx, tPx, rPx, bPx;
        boolean isOpenClearWall = (leftSeamGx <= 2 && rightSeamGx >= GW - 3
                && topSeamGy <= 2 && bottomSeamGy >= GH - 3
                && interiorTotal > 100 && (interiorValid / (float) interiorTotal) >= 0.90f);

        if (isOpenClearWall) {
            // Open unobstructed wall -> preserve 100% of factory 3D trapezoid size!
            lPx = 0;
            tPx = 0;
            rPx = PatternOverlayView.PROJ_W;
            bPx = PatternOverlayView.PROJ_H;
        } else {
            // 9. Optimal 16:9 unobstructed rectangle centered cleanly inside [leftSeamGx..rightSeamGx]
            int[] bestRectCells = findOptimal16x9Rect(safeValid, leftSeamGx, rightSeamGx, topSeamGy, bottomSeamGy);
            int gx0 = bestRectCells[0], gy0 = bestRectCells[1], gx1 = bestRectCells[2], gy1 = bestRectCells[3];

            // Snap to full edge if no seam was detected on that side
            lPx = (gx0 <= 2 && leftSeamGx <= 2) ? 0 : clamp(gx0 * CELL_PX, 0, 1280);
            tPx = (gy0 <= 2 && topSeamGy <= 2) ? 0 : clamp(gy0 * CELL_PX, 0, 720);
            rPx = (gx1 >= GW - 3 && rightSeamGx >= GW - 3)
                    ? PatternOverlayView.PROJ_W : clamp((gx1 + 1) * CELL_PX, lPx + 480, PatternOverlayView.PROJ_W);
            bPx = (gy1 >= GH - 3 && bottomSeamGy >= GH - 3)
                    ? PatternOverlayView.PROJ_H : clamp((gy1 + 1) * CELL_PX, tPx + 270, PatternOverlayView.PROJ_H);

            // Enforce strict exact 16:9 aspect ratio inside [lPx..rPx] x [tPx..bPx] on EVERY path!
            int availW = rPx - lPx;
            int availH = bPx - tPx;
            int exactW = Math.min(availW, Math.round(availH * 16f / 9f));
            int exactH = Math.round(exactW * 9f / 16f);
            exactW = Math.round(exactH * 16f / 9f);
            int cx = (lPx + rPx) / 2;
            int cy = (tPx + bPx) / 2;
            lPx = clamp(cx - exactW / 2, lPx, rPx - exactW);
            rPx = lPx + exactW;
            tPx = clamp(cy - exactH / 2, tPx, bPx - exactH);
            bPx = tPx + exactH;
        }

        float areaRatio = ((rPx - lPx) * (bPx - tPx)) / (float) (PatternOverlayView.PROJ_W * PatternOverlayView.PROJ_H);
        long solveMs = (System.nanoTime() - t0) / 1_000_000L;

        if (areaRatio < minAreaRatio) {
            Log.w(TAG, "Area ratio " + areaRatio + " below floor " + minAreaRatio + "; clamping to safe 16:9 floor.");
            int targetH = Math.min(1080, (int) Math.round(Math.sqrt(minAreaRatio * 1920.0 * 1080.0 * 9.0 / 16.0)));
            int targetW = Math.round(targetH * 16f / 9f);
            int cx = ((leftSeamGx + rightSeamGx + 1) * CELL_PX) / 2;
            int cy = ((topSeamGy + bottomSeamGy + 1) * CELL_PX) / 2;
            lPx = clamp(cx - targetW / 2, 0, 1920 - targetW);
            tPx = clamp(cy - targetH / 2, 0, 1080 - targetH);
            rPx = lPx + targetW;
            bPx = tPx + targetH;
        }

        // Compose rectified 16:9 alcove crop [lPx..rPx] x [tPx..bPx] onto factory 3D trapezoid `baseTrapOptional`!
        int[] insets = composeTrapezoidAndCrop(baseTrapOptional, lPx, tPx, rPx, bPx);

        String summary = String.format(
                "Alcove 16:9 [%d..%d x %d..%d] (%dx%d, %.0f%%) | Walls L=%d R=%d T=%d B=%d | Corners=%d | %dms",
                lPx, rPx, tPx, bPx, (rPx - lPx), (bPx - tPx),
                100f * ((rPx - lPx) * (bPx - tPx)) / (1920f * 1080f),
                leftSeamGx * CELL_PX, (rightSeamGx + 1) * CELL_PX,
                topSeamGy * CELL_PX, (bottomSeamGy + 1) * CELL_PX,
                matchedDots, solveMs);
        Log.i(TAG, summary);

        Runnable debugSaver = null;
        if (saveDebug) {
            final int[] fW = W.clone();
            final double[] fHback = Hback;
            final int fLPx = lPx, fTPx = tPx, fRPx = rPx, fBPx = bPx;
            final int fLeftGx = leftSeamGx, fRightGx = rightSeamGx, fTopGy = topSeamGy, fBotGy = bottomSeamGy;
            final int[] fInsets = insets;
            final String fSummary = summary;
            debugSaver = new Runnable() {
                @Override
                public void run() {
                    saveCheckerDiagnostic(fW, corners, gridToCorner, dotParallax, fHback,
                            safeValid, fLeftGx, fRightGx, fTopGy, fBotGy,
                            fLPx, fTPx, fRPx, fBPx, fInsets, fSummary);
                }
            };
        }

        return new Result(
                true, insets, lPx, tPx, rPx, bPx,
                leftSeamGx * CELL_PX, (rightSeamGx + 1) * CELL_PX,
                topSeamGy * CELL_PX, (bottomSeamGy + 1) * CELL_PX,
                matchedDots, solveMs, summary, debugSaver);
    }

    /**
     * Detects the vertical boundary between the bright central projection wall and darker grey
     * angled side walls / protruding pillars (e.g., +54% jump at left pillar, -32% drop at right sidewall),
     * reinforced by 3D checkerboard homography parallax kink and cone-height slope break.
     */
    private static int detectBrightPlateauVerticalSeam(
            float[][] normW, float[][] projW, float[][] projParallax,
            float[][] projSharp, float[][] projDfd,
            int beamThresh, float refSharp, boolean scanLeft) {

        int[] colSpan = new int[GW];
        for (int x = 0; x < GW; x++) {
            int t = -1, b = -1;
            for (int y = 0; y < GH / 2; y++) {
                if (projW[x][y] >= beamThresh * 1.15f) { t = y; break; }
            }
            for (int y = GH - 1; y > GH / 2; y--) {
                if (projW[x][y] >= beamThresh * 1.15f) { b = y; break; }
            }
            colSpan[x] = (t >= 0 && b > t) ? (b - t) : 0;
        }
        int refSpan = colSpan[GW / 2] > 0 ? colSpan[GW / 2] : GH - 8;

        // Dynamically locate the brightest central plateau column so repointing left/right
        // always anchors inside the true bright main wall!
        float[] colMean = new float[GW];
        for (int x = 20; x < GW - 20; x++) {
            float s = 0; int cnt = 0;
            for (int y = 22; y < GH - 22; y++) {
                if (projW[x][y] >= beamThresh * 1.15f) { s += normW[x][y]; cnt++; }
            }
            colMean[x] = cnt > 16 ? (s / cnt) : 0f;
        }
        int plateauCenterGx = GW / 2;
        float bestBand = -1f;
        for (int x = 42; x <= GW - 43; x++) {
            float band = 0f;
            for (int d = -12; d <= 12; d++) band += colMean[x + d];
            // Mild preference toward optical center so two equally bright flat zones stay centered
            band *= (1.0f - 0.08f * Math.abs(x - GW * 0.5f) / (GW * 0.5f));
            if (band > bestBand) { bestBand = band; plateauCenterGx = x; }
        }

        int defaultEdge = scanLeft ? 1 : (GW - 2);
        int startGx = scanLeft ? clamp(plateauCenterGx - 12, 24, 96) : clamp(plateauCenterGx + 12, 90, GW - 25);
        // Stop 10 cells (100px) inside optical edges so the beam's own outer drop-off never triggers a fake seam!
        int endGx = scanLeft ? 10 : (GW - 11);
        int step = scanLeft ? -1 : 1;

        float[] scores = new float[GW];
        float maxScore = 0f;

        for (int gx = startGx; gx != endGx; gx += step) {
            int inStart = scanLeft ? (gx + 1) : (gx - 5);
            int outStart = scanLeft ? (gx - 5) : (gx + 1);
            if (inStart < 2 || inStart + 4 >= GW - 2 || outStart < 2 || outStart + 4 >= GW - 2) continue;

            float sumInW = 0, sumOutW = 0, sumEdge = 0, positiveStepSum = 0;
            float sumOutPar = 0, sumInPar = 0, sumOutSharp = 0;
            int sharpCnt = 0, activeRows = 0, unlitOuter = 0;
            int outerCheckCol = scanLeft ? (outStart + 1) : (outStart + 3);

            for (int gy = 14; gy < GH - 14; gy++) {
                if (projW[inStart + 2][gy] < beamThresh * 1.15f) continue;
                if (projW[outerCheckCol][gy] < beamThresh * 1.05f) {
                    // Reached the outer optical boundary of the projected beam (not a grey sidewall!)
                    unlitOuter++;
                    continue;
                }
                activeRows++;

                float inV = 0, outV = 0;
                for (int k = 0; k < 5; k++) {
                    inV += normW[inStart + k][gy];
                    outV += normW[outStart + k][gy];
                }
                inV *= 0.2f;
                outV *= 0.2f;

                sumInW += inV;
                sumOutW += outV;
                // Positive step = inner back wall is BRIGHTER than outer grey side wall / pillar!
                float diff = inV - outV;
                sumEdge += Math.abs(diff);
                positiveStepSum += diff;

                sumInPar += projParallax[inStart + 2][gy];
                sumOutPar += projParallax[outStart + 2][gy];
                sumOutSharp += projSharp[outStart + 2][gy];
                sharpCnt++;
            }

            if (unlitOuter > (activeRows + unlitOuter) * 0.18f) {
                // Stop before optical beam drop-off; never invent a false seam at the beam edge!
                break;
            }
            if (activeRows < 22) continue;

            float meanInW = sumInW / activeRows;
            float meanOutW = Math.max(0.05f, sumOutW / activeRows);
            float brightnessDropRatio = meanInW / meanOutW; // e.g., 1.54 at left pillar, 1.47 at right sidewall!
            float coherence = positiveStepSum / Math.max(1e-4f, sumEdge); // +1.0 when inner is consistently brighter!
            float relDrop = (meanInW - meanOutW) / Math.max(0.25f, meanInW);

            float meanOutPar = sumOutPar / activeRows;
            float parRise = Math.max(0f, meanOutPar - (sumInPar / activeRows));
            float sharpRatio = (sharpCnt > 8 && refSharp > 0.04f)
                    ? ((sumOutSharp / sharpCount(sharpCnt)) / refSharp) : 1.0f;

            int farOutGx = clamp(gx + step * 8, 2, GW - 3);
            int nearInGx = clamp(gx - step * 4, 2, GW - 3);
            float spanShrink = (colSpan[farOutGx] > 20 && colSpan[nearInGx] > 20 && refSpan > 40)
                    ? Math.max(0f, (colSpan[nearInGx] - colSpan[farOutGx]) / (float) refSpan) : 0f;

            float score = 0f;
            // Direct bright-backwall -> grey-sidewall vertical step (coherence > 0.80 and >= 19% drop)
            if (coherence > 0.80f && brightnessDropRatio >= 1.19f && relDrop >= 0.16f) {
                score = relDrop * coherence * 5.0f;
                if (spanShrink > 0.020f) score += spanShrink * 8.0f;
                if (sharpRatio < 0.85f) score += (0.86f - sharpRatio) * 1.5f;
            }

            scores[gx] = score;
            if (score > maxScore) maxScore = score;
        }

        if (maxScore < 0.72f) return defaultEdge;

        // Lock onto the FIRST (innermost) clear wall-crease peak (>= 0.72) scanning outward from center!
        // This stops immediately at the alcove wall corner instead of marching onto outer switchboards.
        int bestGx = defaultEdge;
        for (int gx = startGx; gx != endGx; gx += step) {
            float s = scores[gx];
            if (s >= 0.72f
                    && s >= scores[clamp(gx - 1, 0, GW - 1)]
                    && s >= scores[clamp(gx + 1, 0, GW - 1)]
                    && s >= scores[clamp(gx + 2 * step, 0, GW - 1)]) {
                bestGx = gx;
                break;
            }
        }
        if (bestGx == defaultEdge) {
            for (int gx = startGx; gx != endGx; gx += step) {
                if (scores[gx] == maxScore) { bestGx = gx; break; }
            }
        }
        Log.i(TAG, (scanLeft ? "LEFT" : "RIGHT") + " bright-wall seam locked at X="
                + (bestGx * CELL_PX) + "px (score=" + String.format("%.2f", scores[bestGx]) + ")");
        return bestGx;
    }

    private static int sharpCount(int c) { return Math.max(1, c); }

    private static int detectHorizontalWallSeam(
            float[][] normW, float[][] projW, float[][] projParallax,
            float[][] projSharp, float[][] projDfd,
            int beamThresh, float refSharp, boolean scanTop, int gxMin, int gxMax) {

        int defaultEdge = scanTop ? 1 : (GH - 2);
        int startGy = scanTop ? 40 : 68;
        int endGy = scanTop ? 3 : (GH - 4);
        int step = scanTop ? -1 : 1;

        int x0 = Math.max(8, gxMin + 4);
        int x1 = Math.min(GW - 9, gxMax - 4);
        if (x1 - x0 < 20) {
            x0 = 32; x1 = GW - 32;
        }

        int bestGy = defaultEdge;
        float bestScore = 0f;

        for (int gy = startGy; gy != endGy; gy += step) {
            int inStart = scanTop ? (gy + 1) : (gy - 4);
            int outStart = scanTop ? (gy - 4) : (gy + 1);
            if (inStart < 1 || inStart + 3 >= GH || outStart < 1 || outStart + 3 >= GH) continue;

            float sumInW = 0, sumOutW = 0, sumEdge = 0, consistentSign = 0;
            float sumOutPar = 0, sumOutSharp = 0;
            int sharpCnt = 0, activeCols = 0, unlitOuter = 0;

            for (int gx = x0; gx <= x1; gx++) {
                if (projW[gx][inStart + 1] < beamThresh) continue;
                activeCols++;
                if (projW[gx][outStart + 1] < beamThresh * 0.9f) unlitOuter++;

                float inV = 0.25f * (normW[gx][inStart] + normW[gx][inStart + 1] + normW[gx][inStart + 2] + normW[gx][inStart + 3]);
                float outV = 0.25f * (normW[gx][outStart] + normW[gx][outStart + 1] + normW[gx][outStart + 2] + normW[gx][outStart + 3]);
                sumInW += inV;
                sumOutW += outV;
                float d = inV - outV;
                sumEdge += Math.abs(d);
                consistentSign += d;
                sumOutPar += projParallax[gx][outStart + 1];
                if (!isInsideCentralAnchorHalo(gx, outStart + 1)) {
                    sumOutSharp += projSharp[gx][outStart + 1];
                    sharpCnt++;
                }
            }

            if (unlitOuter > (activeCols + unlitOuter) * 0.18f) {
                break;
            }
            if (activeCols < 18) continue;

            float meanInW = sumInW / activeCols;
            float meanOutW = Math.max(0.05f, sumOutW / activeCols);
            float brightnessDropRatio = meanInW / meanOutW;
            // Require inner wall to be GENUINELY much brighter than outer soffit/ledge (never trigger on gentle vertical lens vignetting!)
            float coherence = consistentSign / Math.max(1e-4f, sumEdge);
            float relDrop = (meanInW - meanOutW) / Math.max(0.25f, meanInW);
            float meanPar = sumOutPar / activeCols;
            float sharpRatio = (sharpCnt > 8 && refSharp > 0.05f) ? ((sumOutSharp / sharpCnt) / refSharp) : 1.0f;

            float score = 0f;
            if (coherence > 0.84f && brightnessDropRatio >= 1.28f && relDrop >= 0.22f
                    && (sharpRatio < 0.80f || meanPar > 2.2f || relDrop >= 0.30f)) {
                score = relDrop * coherence * 4.5f + Math.max(0f, meanPar - 1.8f) * 0.6f + Math.max(0f, 0.82f - sharpRatio) * 1.5f;
            }
            if (score > 0.85f && score > bestScore) {
                bestScore = score;
                bestGy = gy;
            } else if (bestScore > 0.95f && Math.abs(gy - bestGy) >= 5 && score < bestScore * 0.6f) {
                break;
            }
        }
        if (bestGy != defaultEdge) {
            Log.i(TAG, (scanTop ? "TOP" : "BOTTOM") + " horizontal seam locked at Y="
                    + (bestGy * CELL_PX) + "px (score=" + String.format("%.2f", bestScore) + ")");
        }
        return bestGy;
    }

    /**
     * Exact O(GW * GH) monotonic-stack largest 16:9 unobstructed rectangle solver,
     * weighted slightly toward optical center to maximize lens corner sharpness.
     */
    private static int[] findOptimal16x9Rect(boolean[][] valid, int gxMin, int gxMax, int gyMin, int gyMax) {
        int[] heights = new int[GW];
        int[] stack = new int[GW + 1];

        double bestScore = -1.0;
        int bestL = Math.max(0, gxMin), bestT = Math.max(0, gyMin);
        int bestR = Math.min(GW - 1, gxMax), bestB = Math.min(GH - 1, gyMax);

        for (int gy = 0; gy < GH; gy++) {
            for (int gx = 0; gx < GW; gx++) {
                heights[gx] = valid[gx][gy] ? (heights[gx] + 1) : 0;
            }
            int top = -1;
            for (int gx = 0; gx <= GW; gx++) {
                int h = (gx == GW) ? 0 : heights[gx];
                while (top >= 0 && h < heights[stack[top]]) {
                    int rectH = heights[stack[top--]];
                    int rectL = (top < 0) ? 0 : (stack[top] + 1);
                    int rectR = gx - 1;
                    int rectW = rectR - rectL + 1;
                    int rectT = gy - rectH + 1;
                    int rectB = gy;

                    // Fit largest 16:9 rectangle inside [rectL..rectR] x [rectT..rectB]
                    int w16 = Math.min(rectW, (rectH * 16) / 9);
                    int h9 = (w16 * 9) / 16;
                    if (h9 < 18 || w16 < 32) continue;
                    w16 = (h9 * 16) / 9;

                    // Center horizontally within the detected alcove wall span [gxMin..gxMax]
                    float targetCenterX = (gxMin + gxMax) * 0.5f;
                    int idealL = Math.round(targetCenterX - w16 * 0.5f);
                    int subL = clamp(idealL, rectL, rectR - w16 + 1);
                    int subR = subL + w16 - 1;

                    float targetCenterY = (gyMin + gyMax) * 0.5f;
                    int idealT = Math.round(targetCenterY - h9 * 0.5f);
                    int subT = clamp(idealT, rectT, rectB - h9 + 1);
                    int subB = subT + h9 - 1;

                    double area = (double) w16 * h9;
                    double cx = (subL + subR) * 0.5;
                    double cy = (subT + subB) * 0.5;
                    double centerDist = Math.hypot((cx - targetCenterX) / GW, (cy - targetCenterY) / GH);
                    // Primary objective: maximize unobstructed 16:9 area; tie-break: center cleanly in alcove
                    double score = area * (1.0 - 0.04 * centerDist);
                    if (score > bestScore) {
                        bestScore = score;
                        bestL = subL;
                        bestT = subT;
                        bestR = subR;
                        bestB = subB;
                    }
                }
                stack[++top] = gx;
            }
        }
        if (bestScore < 0) {
            // Ironclad 16:9 fallback inside [gxMin..gxMax] x [gyMin..gyMax]
            int fw = Math.max(32, bestR - bestL + 1);
            int fh = Math.max(18, bestB - bestT + 1);
            int w16 = Math.min(fw, (fh * 16) / 9);
            int h9 = (w16 * 9) / 16;
            w16 = (h9 * 16) / 9;
            int cx = (bestL + bestR) / 2;
            int cy = (bestT + bestB) / 2;
            bestL = clamp(cx - w16 / 2, 0, GW - w16);
            bestR = bestL + w16 - 1;
            bestT = clamp(cy - h9 / 2, 0, GH - h9);
            bestB = bestT + h9 - 1;
        }
        return new int[] { bestL, bestT, bestR, bestB };
    }

    private static int[] minFilterSeparable(int[] src, int radius) {
        int[] tmp = new int[CAM_N];
        int[] out = new int[CAM_N];
        for (int v = 0; v < CAM_H; v++) {
            int off = v * CAM_W;
            for (int u = 0; u < CAM_W; u++) {
                int m = 255;
                int u0 = Math.max(0, u - radius), u1 = Math.min(CAM_W - 1, u + radius);
                for (int x = u0; x <= u1; x++) {
                    int val = src[off + x];
                    if (val < m) m = val;
                }
                tmp[off + u] = m;
            }
        }
        for (int v = 0; v < CAM_H; v++) {
            int v0 = Math.max(0, v - radius), v1 = Math.min(CAM_H - 1, v + radius);
            for (int u = 0; u < CAM_W; u++) {
                int m = 255;
                for (int y = v0; y <= v1; y++) {
                    int val = tmp[y * CAM_W + u];
                    if (val < m) m = val;
                }
                out[v * CAM_W + u] = m;
            }
        }
        return out;
    }

    /**
     * O(1)-per-pixel van Herk / Gil-Werman separable max filter (<= 3 ops/pixel regardless of radius!).
     * Zero heap allocations per scan!
     */
    private static int[] maxFilterSeparable(int[] src, int radius, int[] tmp, int[] out) {
        final int win = 2 * radius + 1;
        int[] pref = new int[CAM_W];
        int[] suff = new int[CAM_W];

        for (int v = 0; v < CAM_H; v++) {
            int off = v * CAM_W;
            for (int u = 0; u < CAM_W; u++) {
                int val = src[off + u];
                pref[u] = (u % win == 0) ? val : Math.max(pref[u - 1], val);
            }
            for (int u = CAM_W - 1; u >= 0; u--) {
                int val = src[off + u];
                suff[u] = (u == CAM_W - 1 || (u + 1) % win == 0) ? val : Math.max(suff[u + 1], val);
            }
            for (int u = 0; u < CAM_W; u++) {
                int lo = Math.max(0, u - radius), hi = Math.min(CAM_W - 1, u + radius);
                tmp[off + u] = (lo / win == hi / win)
                        ? ((lo == 0) ? pref[hi] : suff[lo])
                        : Math.max(suff[lo], pref[hi]);
            }
        }

        int[] prefY = new int[CAM_H];
        int[] suffY = new int[CAM_H];
        for (int u = 0; u < CAM_W; u++) {
            for (int v = 0; v < CAM_H; v++) {
                int val = tmp[v * CAM_W + u];
                prefY[v] = (v % win == 0) ? val : Math.max(prefY[v - 1], val);
            }
            for (int v = CAM_H - 1; v >= 0; v--) {
                int val = tmp[v * CAM_W + u];
                suffY[v] = (v == CAM_H - 1 || (v + 1) % win == 0) ? val : Math.max(suffY[v + 1], val);
            }
            for (int v = 0; v < CAM_H; v++) {
                int lo = Math.max(0, v - radius), hi = Math.min(CAM_H - 1, v + radius);
                out[v * CAM_W + u] = (lo / win == hi / win)
                        ? ((lo == 0) ? prefY[hi] : suffY[lo])
                        : Math.max(suffY[lo], prefY[hi]);
            }
        }
        return out;
    }

    private static List<CornerNode> detectCheckerCorners(int[] S, int[] W, int[] origDiff, int beamThresh) {
        final int d = 4;
        final float[] resp = BUF_RESP;
        final int[] sign = BUF_SIGN;
        Arrays.fill(resp, 0f);

        for (int v = d + 3; v < CAM_H - d - 3; v++) {
            for (int u = d + 3; u < CAM_W - d - 3; u++) {
                int idx = v * CAM_W + u;
                int w = W[idx];
                if (w < beamThresh) continue;

                int tl = S[(v - d) * CAM_W + (u - d)];
                int tr = S[(v - d) * CAM_W + (u + d)];
                int bl = S[(v + d) * CAM_W + (u - d)];
                int br = S[(v + d) * CAM_W + (u + d)];

                if ((tl * br) <= 0 || (tr * bl) <= 0 || (tl * tr) >= 0) continue;

                float c = 0.25f * ((tl + br) - (tr + bl));
                float ac = Math.abs(c);
                if (ac < Math.max(6.5f, w * 0.24f)) continue;
                resp[idx] = ac;
                sign[idx] = c > 0 ? 1 : -1;
            }
        }

        List<CornerNode> out = new ArrayList<>(240);
        final int nms = 5;
        for (int v = d + nms; v < CAM_H - d - nms; v++) {
            for (int u = d + nms; u < CAM_W - d - nms; u++) {
                int idx = v * CAM_W + u;
                float val = resp[idx];
                if (val <= 0f) continue;
                boolean isMax = true;
                double sumW = 0, sumU = 0, sumV = 0;
                for (int dv = -nms; dv <= nms && isMax; dv++) {
                    int ro = (v + dv) * CAM_W;
                    for (int du = -nms; du <= nms; du++) {
                        float nv = resp[ro + u + du];
                        if (nv > val) { isMax = false; break; }
                        if (Math.abs(du) <= 2 && Math.abs(dv) <= 2 && nv > 0f) {
                            sumW += nv;
                            sumU += (u + du) * nv;
                            sumV += (v + dv) * nv;
                        }
                    }
                }
                if (!isMax) continue;

                CornerNode cn = new CornerNode();
                cn.u = sumW > 0 ? (float) (sumU / sumW) : u;
                cn.v = sumW > 0 ? (float) (sumV / sumW) : v;
                cn.parity = sign[idx];
                cn.strength = val;

                int qu = Math.round(cn.u), qv = Math.round(cn.v);
                int qd = 6;
                if (qu >= qd && qu < CAM_W - qd && qv >= qd && qv < CAM_H - qd) {
                    float oTL = origDiff[(qv - qd) * CAM_W + (qu - qd)];
                    float oTR = origDiff[(qv - qd) * CAM_W + (qu + qd)];
                    float oBL = origDiff[(qv + qd) * CAM_W + (qu - qd)];
                    float oBR = origDiff[(qv + qd) * CAM_W + (qu + qd)];
                    float minThree = Math.min(oTL, Math.min(oTR, oBL));
                    cn.originScore = (cn.parity > 0) ? (minThree - oBR * 1.4f) : -999f;
                }
                out.add(cn);
            }
        }
        return out;
    }

    private static int assignCheckerLattice(List<CornerNode> corners, int[][] gridToCorner, int wP85) {
        final int n = corners.size();
        if (n < 8) return 0;

        // Try up to top-3 origin candidates so a speck/occlusion near (960,540) doesn't abort lattice indexing!
        float minOriginScore = Math.max(5.5f, wP85 * 0.18f);
        int[] topOrigins = { -1, -1, -1 };
        float[] topScores = { minOriginScore, minOriginScore, minOriginScore };
        for (int i = 0; i < n; i++) {
            float sc = corners.get(i).originScore;
            if (sc > topScores[0]) {
                topScores[2] = topScores[1]; topOrigins[2] = topOrigins[1];
                topScores[1] = topScores[0]; topOrigins[1] = topOrigins[0];
                topScores[0] = sc; topOrigins[0] = i;
            } else if (sc > topScores[1]) {
                topScores[2] = topScores[1]; topOrigins[2] = topOrigins[1];
                topScores[1] = sc; topOrigins[1] = i;
            } else if (sc > topScores[2]) {
                topScores[2] = sc; topOrigins[2] = i;
            }
        }

        final int nCols = PatternOverlayView.TILE_COLS;
        final int nRows = PatternOverlayView.TILE_ROWS;
        int bestMatched = 0;
        int[][] bestGrid = null;

        for (int candOrd = 0; candOrd < 3; candOrd++) {
            int bestO = topOrigins[candOrd];
            if (bestO < 0) break;

            for (int c = 0; c < nCols; c++) Arrays.fill(gridToCorner[c], -1);
            for (int i = 0; i < n; i++) {
                CornerNode node = corners.get(i);
                node.assignedCol = -1;
                node.assignedRow = -1;
            }

            CornerNode co = corners.get(bestO);
            int bestX = -1, bestY = -1;
            float bestDx = 999f, bestDy = 999f;
            for (int i = 0; i < n; i++) {
                if (i == bestO) continue;
                CornerNode b = corners.get(i);
                if (b.parity != -co.parity) continue;
                float du = b.u - co.u, dv = b.v - co.v;
                float dist = (float) Math.hypot(du, dv);
                if (dist < 8f || dist > 58f) continue;
                if (du > Math.abs(dv) * 1.20f && dist < bestDx) { bestDx = dist; bestX = i; }
                if (dv > Math.abs(du) * 1.20f && dist < bestDy) { bestDy = dist; bestY = i; }
            }
            if (bestX < 0 || bestY < 0) continue;

            int oc = PatternOverlayView.ORIGIN_CORNER_COL;
            int or = PatternOverlayView.ORIGIN_CORNER_ROW;

            gridToCorner[oc][or] = bestO; co.assignedCol = oc; co.assignedRow = or;
            CornerNode cx = corners.get(bestX);
            gridToCorner[oc + 1][or] = bestX; cx.assignedCol = oc + 1; cx.assignedRow = or;
            CornerNode cy = corners.get(bestY);
            gridToCorner[oc][or + 1] = bestY; cy.assignedCol = oc; cy.assignedRow = or + 1;

            float globalExU = cx.u - co.u, globalExV = cx.v - co.v;
            float globalEyU = cy.u - co.u, globalEyV = cy.v - co.v;
            float stepX = (float) Math.hypot(globalExU, globalExV);
            float stepY = (float) Math.hypot(globalEyU, globalEyV);

            int[] qC = new int[256], qR = new int[256];
            int qH = 0, qT = 0;
            qC[qT] = oc; qR[qT++] = or;
            qC[qT] = oc + 1; qR[qT++] = or;
            qC[qT] = oc; qR[qT++] = or + 1;

            int[][] dirs = { {1, 0}, {-1, 0}, {0, 1}, {0, -1} };
            int matched = 3;

            while (qH < qT) {
                int c = qC[qH], r = qR[qH++];
                CornerNode cur = corners.get(gridToCorner[c][r]);

                for (int[] d : dirs) {
                    int nc = c + d[0], nr = r + d[1];
                    if (nc < 1 || nc >= nCols || nr < 1 || nr >= nRows) continue;
                    if (gridToCorner[nc][nr] >= 0) continue;

                    float su, sv;
                    int bc = c - d[0], br = r - d[1];
                    if (bc >= 1 && bc < nCols && br >= 1 && br < nRows && gridToCorner[bc][br] >= 0) {
                        CornerNode prev = corners.get(gridToCorner[bc][br]);
                        su = cur.u - prev.u;
                        sv = cur.v - prev.v;
                    } else {
                        su = d[0] * globalExU + d[1] * globalEyU;
                        sv = d[0] * globalExV + d[1] * globalEyV;
                    }
                    float predU = cur.u + su, predV = cur.v + sv;
                    float tol = Math.max(6.5f, 0.42f * (d[0] != 0 ? stepX : stepY));
                    int expectedParity = (((nc + nr) & 1) == 0) ? 1 : -1;

                    int bestIdx = -1;
                    float bestDist2 = tol * tol;
                    for (int i = 0; i < n; i++) {
                        CornerNode cand = corners.get(i);
                        if (cand.assignedCol >= 0 || cand.parity != expectedParity) continue;
                        float du = cand.u - predU, dv = cand.v - predV;
                        float d2 = du * du + dv * dv;
                        if (d2 < bestDist2) { bestDist2 = d2; bestIdx = i; }
                    }
                    if (bestIdx >= 0 && qT < qC.length) {
                        CornerNode hit = corners.get(bestIdx);
                        gridToCorner[nc][nr] = bestIdx;
                        hit.assignedCol = nc;
                        hit.assignedRow = nr;
                        qC[qT] = nc; qR[qT++] = nr;
                        matched++;
                    }
                }
            }
            if (matched >= 24) {
                return matched;
            }
            if (matched > bestMatched) {
                bestMatched = matched;
                bestGrid = new int[nCols][nRows];
                for (int c = 0; c < nCols; c++) System.arraycopy(gridToCorner[c], 0, bestGrid[c], 0, nRows);
            }
        }
        if (bestGrid != null) {
            for (int c = 0; c < nCols; c++) System.arraycopy(bestGrid[c], 0, gridToCorner[c], 0, nRows);
        }
        return bestMatched;
    }

    private static boolean isHomographyPlausible(double[] H) {
        if (H == null) return false;
        double[] c0 = new double[2], c1 = new double[2], c2 = new double[2], c3 = new double[2];
        HomographySolver.project(H, 0, 0, c0);
        HomographySolver.project(H, 1920, 0, c1);
        HomographySolver.project(H, 1920, 1080, c2);
        HomographySolver.project(H, 0, 1080, c3);
        double wTop = c1[0] - c0[0], wBot = c2[0] - c3[0];
        double hLeft = c3[1] - c0[1], hRight = c2[1] - c1[1];
        return wTop > 110 && wTop < 740 && wBot > 110 && wBot < 740
                && hLeft > 62 && hLeft < 540 && hRight > 62 && hRight < 540
                && c0[0] > -140 && c1[0] < CAM_W + 140 && c0[1] > -140 && c3[1] < CAM_H + 140;
    }

    /**
     * O(1)-per-pixel van Herk separable max-filter for checkerboard edge sharpness (~9 ms vs ~140 ms!).
     */
    private static float[] computeCheckerEdgeSharpness(
            int[] S, int[] W, int beamThresh, int normFloor,
            float[] raw, float[] tmp, float[] out) {
        Arrays.fill(raw, 0f);
        for (int v = 2; v < CAM_H - 2; v++) {
            int off = v * CAM_W;
            for (int u = 2; u < CAM_W - 2; u++) {
                int i = off + u;
                if (W[i] < beamThresh) continue;
                int g = Math.abs(S[i + 1] - S[i - 1]);
                raw[i] = g / (float) (2 * Math.max(W[i], normFloor));
            }
        }
        final int rx = 14, winX = 2 * rx + 1;
        float[] prefX = new float[CAM_W], suffX = new float[CAM_W];

        for (int v = 0; v < CAM_H; v++) {
            int off = v * CAM_W;
            for (int u = 0; u < CAM_W; u++) {
                float val = raw[off + u];
                prefX[u] = (u % winX == 0) ? val : Math.max(prefX[u - 1], val);
            }
            for (int u = CAM_W - 1; u >= 0; u--) {
                float val = raw[off + u];
                suffX[u] = (u == CAM_W - 1 || (u + 1) % winX == 0) ? val : Math.max(suffX[u + 1], val);
            }
            for (int u = 0; u < CAM_W; u++) {
                int lo = Math.max(0, u - rx), hi = Math.min(CAM_W - 1, u + rx);
                tmp[off + u] = (lo / winX == hi / winX)
                        ? ((lo == 0) ? prefX[hi] : suffX[lo])
                        : Math.max(suffX[lo], prefX[hi]);
            }
        }

        final int ry = 4, winY = 2 * ry + 1;
        float[] prefY = new float[CAM_H], suffY = new float[CAM_H];
        for (int u = 0; u < CAM_W; u++) {
            for (int v = 0; v < CAM_H; v++) {
                float val = tmp[v * CAM_W + u];
                prefY[v] = (v % winY == 0) ? val : Math.max(prefY[v - 1], val);
            }
            for (int v = CAM_H - 1; v >= 0; v--) {
                float val = tmp[v * CAM_W + u];
                suffY[v] = (v == CAM_H - 1 || (v + 1) % winY == 0) ? val : Math.max(suffY[v + 1], val);
            }
            for (int v = 0; v < CAM_H; v++) {
                int lo = Math.max(0, v - ry), hi = Math.min(CAM_H - 1, v + ry);
                out[v * CAM_W + u] = (lo / winY == hi / winY)
                        ? ((lo == 0) ? prefY[hi] : suffY[lo])
                        : Math.max(suffY[lo], prefY[hi]);
            }
        }
        return out;
    }

    private static float interpolateCheckerParallax(float[][] dotPar, boolean[][] obs, float px, float py) {
        final int nCols = PatternOverlayView.TILE_COLS;
        final int nRows = PatternOverlayView.TILE_ROWS;
        float fc = px / PatternOverlayView.TILE_W;
        float fr = py / PatternOverlayView.TILE_H;
        int c0 = clamp((int) Math.floor(fc), 1, nCols - 2);
        int r0 = clamp((int) Math.floor(fr), 1, nRows - 2);
        float tx = clampFloat(fc - c0, 0f, 1f);
        float ty = clampFloat(fr - r0, 0f, 1f);

        float sum = 0f, wSum = 0f;
        float[] wx = { 1f - tx, tx };
        float[] wy = { 1f - ty, ty };
        for (int dc = 0; dc <= 1; dc++) {
            for (int dr = 0; dr <= 1; dr++) {
                int c = c0 + dc, r = r0 + dr;
                if (obs[c][r]) {
                    float w = wx[dc] * wy[dr] + 0.05f;
                    sum += dotPar[c][r] * w;
                    wSum += w;
                }
            }
        }
        return wSum > 0f ? (sum / wSum) : 0f;
    }

    private static boolean[][] fillSmallHoles5x5(boolean[][] src, int gxMin, int gxMax, int gyMin, int gyMax) {
        boolean[][] out = new boolean[GW][GH];
        for (int x = 0; x < GW; x++) {
            System.arraycopy(src[x], 0, out[x], 0, GH);
        }
        int x0 = Math.max(2, gxMin + 1), x1 = Math.min(GW - 3, gxMax - 1);
        int y0 = Math.max(2, gyMin + 1), y1 = Math.min(GH - 3, gyMax - 1);
        for (int x = x0; x <= x1; x++) {
            for (int y = y0; y <= y1; y++) {
                if (src[x][y]) continue;
                int trues = 0;
                for (int dx = -2; dx <= 2; dx++) {
                    for (int dy = -2; dy <= 2; dy++) {
                        if (src[x + dx][y + dy]) trues++;
                    }
                }
                if (trues >= 17) {
                    out[x][y] = true;
                }
            }
        }
        return out;
    }

    private static boolean[][] morphClose2(boolean[][] src, int gxMin, int gxMax, int gyMin, int gyMax) {
        boolean[][] dil = new boolean[GW][GH];
        boolean openL = gxMin <= 2, openR = gxMax >= GW - 3, openT = gyMin <= 2, openB = gyMax >= GH - 3;
        for (int x = 0; x < GW; x++) {
            for (int y = 0; y < GH; y++) {
                int trues = 0;
                for (int dx = -1; dx <= 1; dx++) {
                    int nx = clamp(x + dx, 0, GW - 1);
                    for (int dy = -1; dy <= 1; dy++) {
                        int ny = clamp(y + dy, 0, GH - 1);
                        if (src[nx][ny]) trues++;
                    }
                }
                boolean edgeKeep = (openL && x <= 1 && y >= gyMin && y <= gyMax)
                        || (openR && x >= GW - 2 && y >= gyMin && y <= gyMax)
                        || (openT && y <= 1 && x >= gxMin && x <= gxMax)
                        || (openB && y >= GH - 2 && x >= gxMin && x <= gxMax);
                dil[x][y] = src[x][y] || trues >= 5 || (edgeKeep && trues >= 3);
            }
        }
        return dil;
    }

    private static double[] fitBeamQuadFallbackHomography(int[] W, int beamThresh) {
        // Find 4 extreme illuminated corners of W > beamThresh via diagonal projections u+v, u-v
        double tlScore = 1e9, brScore = -1e9, trScore = -1e9, blScore = 1e9;
        double tlU = 0, tlV = 0, brU = 0, brV = 0, trU = 0, trV = 0, blU = 0, blV = 0;
        int count = 0;
        for (int v = 4; v < CAM_H - 4; v++) {
            int off = v * CAM_W;
            for (int u = 4; u < CAM_W - 4; u++) {
                if (W[off + u] < Math.max(9f, beamThresh * 1.3f)) continue;
                count++;
                double s1 = u + v;
                double s2 = u - v;
                if (s1 < tlScore) { tlScore = s1; tlU = u; tlV = v; }
                if (s1 > brScore) { brScore = s1; brU = u; brV = v; }
                if (s2 > trScore) { trScore = s2; trU = u; trV = v; }
                if (s2 < blScore) { blScore = s2; blU = u; blV = v; }
            }
        }
        if (count < 220) return null;
        double[] sx = { 0, 1919, 1919, 0 };
        double[] sy = { 0, 0, 1079, 1079 };
        double[] du = { tlU, trU, brU, blU };
        double[] dv = { tlV, trV, brV, blV };
        return HomographySolver.solveLeastSquares(sx, sy, du, dv, 4);
    }

    private static boolean isInsideCentralAnchorHalo(int gx, int gy) {
        float px = (gx + 0.5f) * CELL_PX;
        float py = (gy + 0.5f) * CELL_PX;
        return px >= 770f && px <= 1150f && py >= 345f && py <= 735f;
    }

    /**
     * O(1) prefix-sum separable box filter on GW x GH grid.
     */
    private static float[][] boxSmooth(float[][] src, int radius) {
        float[][] tmp = new float[GW][GH];
        float[][] out = new float[GW][GH];
        float[] pref = new float[Math.max(GW, GH) + 1];

        for (int gy = 0; gy < GH; gy++) {
            pref[0] = 0f;
            for (int gx = 0; gx < GW; gx++) pref[gx + 1] = pref[gx] + src[gx][gy];
            for (int gx = 0; gx < GW; gx++) {
                int lo = Math.max(0, gx - radius), hi = Math.min(GW - 1, gx + radius);
                tmp[gx][gy] = (pref[hi + 1] - pref[lo]) / (float) (hi - lo + 1);
            }
        }
        for (int gx = 0; gx < GW; gx++) {
            pref[0] = 0f;
            for (int gy = 0; gy < GH; gy++) pref[gy + 1] = pref[gy] + tmp[gx][gy];
            for (int gy = 0; gy < GH; gy++) {
                int lo = Math.max(0, gy - radius), hi = Math.min(GH - 1, gy + radius);
                out[gx][gy] = (pref[hi + 1] - pref[lo]) / (float) (hi - lo + 1);
            }
        }
        return out;
    }

    private static boolean[][] erodePreservingOpenBorders(
            boolean[][] src, int radius, int gxMin, int gxMax, int gyMin, int gyMax) {
        boolean[][] out = new boolean[GW][GH];
        boolean openL = gxMin <= 2, openR = gxMax >= GW - 3, openT = gyMin <= 2, openB = gyMax >= GH - 3;
        for (int x = 0; x < GW; x++) {
            for (int y = 0; y < GH; y++) {
                boolean ok = true;
                for (int dx = -radius; dx <= radius && ok; dx++) {
                    int nx = x + dx;
                    if (nx < 0) { if (!openL) { ok = false; break; } nx = 0; }
                    if (nx >= GW) { if (!openR) { ok = false; break; } nx = GW - 1; }
                    for (int dy = -radius; dy <= radius; dy++) {
                        int ny = y + dy;
                        if (ny < 0) { if (!openT) { ok = false; break; } ny = 0; }
                        if (ny >= GH) { if (!openB) { ok = false; break; } ny = GH - 1; }
                        if (!src[nx][ny]) { ok = false; break; }
                    }
                }
                out[x][y] = ok;
            }
        }
        return out;
    }

    private static boolean[][] morphClose1(boolean[][] src) {
        boolean[][] dil = new boolean[GW][GH];
        for (int x = 1; x < GW - 1; x++) {
            for (int y = 1; y < GH - 1; y++) {
                int trues = 0;
                for (int dx = -1; dx <= 1; dx++) {
                    for (int dy = -1; dy <= 1; dy++) {
                        if (src[x + dx][y + dy]) trues++;
                    }
                }
                dil[x][y] = src[x][y] || trues >= 6;
            }
        }
        return dil;
    }

    private static boolean[][] erode(boolean[][] src, int radius) {
        boolean[][] out = new boolean[GW][GH];
        for (int x = radius; x < GW - radius; x++) {
            for (int y = radius; y < GH - radius; y++) {
                boolean ok = true;
                for (int dx = -radius; dx <= radius && ok; dx++) {
                    for (int dy = -radius; dy <= radius; dy++) {
                        if (!src[x + dx][y + dy]) { ok = false; break; }
                    }
                }
                out[x][y] = ok;
            }
        }
        return out;
    }

    private static float percentileGrid(float[][] g, int x0, int x1, int y0, int y1, float q) {
        int cap = Math.max(1, (x1 - x0 + 1) * (y1 - y0 + 1));
        float[] vals = new float[cap];
        int n = 0;
        for (int x = Math.max(0, x0); x <= Math.min(GW - 1, x1); x++) {
            for (int y = Math.max(0, y0); y <= Math.min(GH - 1, y1); y++) {
                if (g[x][y] > 0f) vals[n++] = g[x][y];
            }
        }
        if (n == 0) return 0f;
        Arrays.sort(vals, 0, n);
        return vals[clamp((int) (n * q), 0, n - 1)];
    }

    private static float sampleBilinearInt(int[] img, float u, float v) {
        int u0 = (int) u, v0 = (int) v;
        float fu = u - u0, fv = v - v0;
        int off = v0 * CAM_W + u0;
        float a = img[off] * (1f - fu) + img[off + 1] * fu;
        float b = img[off + CAM_W] * (1f - fu) + img[off + CAM_W + 1] * fu;
        return a * (1f - fv) + b * fv;
    }

    private static float sampleBilinearFloat(float[] img, float u, float v) {
        int u0 = (int) u, v0 = (int) v;
        float fu = u - u0, fv = v - v0;
        int off = v0 * CAM_W + u0;
        float a = img[off] * (1f - fu) + img[off + 1] * fu;
        float b = img[off + CAM_W] * (1f - fu) + img[off + CAM_W + 1] * fu;
        return a * (1f - fv) + b * fv;
    }

    private static int clamp(int v, int lo, int hi) {
        return v < lo ? lo : (v > hi ? hi : v);
    }

    private static float clampFloat(float v, float lo, float hi) {
        return v < lo ? lo : (v > hi ? hi : v);
    }

    private static void saveCheckerDiagnostic(
            int[] W, List<CornerNode> corners, int[][] gridToCorner, float[][] dotParallax, double[] Hback,
            boolean[][] safeValid, int leftGx, int rightGx, int topGy, int botGy,
            int lPx, int tPx, int rPx, int bPx, int[] insets, String summary) {
        try {
            Bitmap bmp = Bitmap.createBitmap(1280, 480, Bitmap.Config.ARGB_8888);
            int[] px = new int[CAM_N];
            for (int i = 0; i < CAM_N; i++) {
                int g = clamp((W[i] * 3) / 2, 0, 255);
                px[i] = 0xFF000000 | (g << 16) | (g << 8) | g;
            }
            bmp.setPixels(px, 0, CAM_W, 0, 0, CAM_W, CAM_H);

            Canvas cv = new Canvas(bmp);
            Paint p = new Paint(Paint.ANTI_ALIAS_FLAG);
            for (int c = 1; c < PatternOverlayView.TILE_COLS; c++) {
                for (int r = 1; r < PatternOverlayView.TILE_ROWS; r++) {
                    int ci = gridToCorner[c][r];
                    if (ci < 0) continue;
                    CornerNode cn = corners.get(ci);
                    p.setStyle(Paint.Style.FILL);
                    p.setColor(dotParallax[c][r] > 2.0f ? 0xFFFF5252 : 0xFF00E676);
                    cv.drawCircle(cn.u, cn.v, 3.5f, p);
                }
            }

            double[] q0 = new double[2], q1 = new double[2], q2 = new double[2], q3 = new double[2];
            HomographySolver.project(Hback, lPx, tPx, q0);
            HomographySolver.project(Hback, rPx, tPx, q1);
            HomographySolver.project(Hback, rPx, bPx, q2);
            HomographySolver.project(Hback, lPx, bPx, q3);
            p.setStyle(Paint.Style.STROKE);
            p.setStrokeWidth(2.5f);
            p.setColor(0xFFFFEA00);
            cv.drawLine((float) q0[0], (float) q0[1], (float) q1[0], (float) q1[1], p);
            cv.drawLine((float) q1[0], (float) q1[1], (float) q2[0], (float) q2[1], p);
            cv.drawLine((float) q2[0], (float) q2[1], (float) q3[0], (float) q3[1], p);
            cv.drawLine((float) q3[0], (float) q3[1], (float) q0[0], (float) q0[1], p);

            float ox = 660f, oy = 50f, sw = 600f, sh = 338f;
            p.setStyle(Paint.Style.FILL);
            p.setColor(0xFF141A22);
            cv.drawRect(640, 0, 1280, 480, p);

            float cw = sw / GW, ch = sh / GH;
            for (int gx = 0; gx < GW; gx++) {
                for (int gy = 0; gy < GH; gy++) {
                    p.setColor(safeValid[gx][gy] ? 0xFF264E36 : 0xFF4A1E24);
                    cv.drawRect(ox + gx * cw, oy + gy * ch, ox + (gx + 1) * cw + 0.5f, oy + (gy + 1) * ch + 0.5f, p);
                }
            }

            p.setStyle(Paint.Style.STROKE);
            p.setStrokeWidth(2.5f);
            p.setColor(0xFF00E5FF);
            cv.drawLine(ox + leftGx * cw, oy, ox + leftGx * cw, oy + sh, p);
            cv.drawLine(ox + (rightGx + 1) * cw, oy, ox + (rightGx + 1) * cw, oy + sh, p);
            cv.drawLine(ox, oy + topGy * ch, ox + sw, oy + topGy * ch, p);
            cv.drawLine(ox, oy + (botGy + 1) * ch, ox + sw, oy + (botGy + 1) * ch, p);

            p.setColor(0xFFFFEA00);
            p.setStrokeWidth(3f);
            cv.drawRect(
                    ox + (lPx / 1920f) * sw,
                    oy + (tPx / 1080f) * sh,
                    ox + (rPx / 1920f) * sw,
                    oy + (bPx / 1080f) * sh, p);

            p.setStyle(Paint.Style.FILL);
            p.setColor(Color.WHITE);
            p.setTextSize(15f);
            cv.drawText(summary, 20f, 462f, p);
            cv.drawText("Projector 1920x1080 Map (Green=Bright Center Wall, Red=Grey Sidewall/Obstacle, Cyan=Seam, Yellow=16:9)",
                    650f, 32f, p);

            File pngFile = new File("/sdcard/alcove_debug.png");
            try (FileOutputStream fos = new FileOutputStream(pngFile)) {
                bmp.compress(Bitmap.CompressFormat.PNG, 100, fos);
            }
            bmp.recycle();

            try (FileWriter fw = new FileWriter("/sdcard/alcove_last_report.txt", false)) {
                fw.write(summary + "\n");
                fw.write("setkey=" + Arrays.toString(insets) + "\n");
            }
        } catch (Throwable t) {
            Log.w(TAG, "Failed saving diagnostic PNG: " + t.getMessage());
        }
    }
}
