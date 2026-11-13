package com.hysd.hyscreen.alcove;

import java.util.Random;

/**
 * Fast, allocation-minimal planar homography solver (Hartley-normalized DLT + RANSAC + 3x3 inversion)
 * for mapping 1920x1080 projector coordinates (X, Y) <-> 640x480 camera coordinates (u, v).
 */
public final class HomographySolver {
    private HomographySolver() {}

    public static void project(double[] H, double x, double y, double[] outUv) {
        double w = H[6] * x + H[7] * y + H[8];
        if (Math.abs(w) < 1e-12) w = 1e-12;
        outUv[0] = (H[0] * x + H[1] * y + H[2]) / w;
        outUv[1] = (H[3] * x + H[4] * y + H[5]) / w;
    }

    public static double[] invert3x3(double[] m) {
        double a = m[0], b = m[1], c = m[2];
        double d = m[3], e = m[4], f = m[5];
        double g = m[6], h = m[7], i = m[8];

        double A = (e * i - f * h);
        double B = -(d * i - f * g);
        double C = (d * h - e * g);
        double D = -(b * i - c * h);
        double E = (a * i - c * g);
        double F = -(a * h - b * g);
        double G = (b * f - c * e);
        double Hh = -(a * f - c * d);
        double I = (a * e - b * d);

        double det = a * A + b * B + c * C;
        if (Math.abs(det) < 1e-14) return null;
        double invDet = 1.0 / det;
        return new double[] {
            A * invDet, D * invDet, G * invDet,
            B * invDet, E * invDet, Hh * invDet,
            C * invDet, F * invDet, I * invDet
        };
    }

    /**
     * Least-squares Hartley-normalized homography H (3x3 row-major) from N >= 4 point pairs.
     */
    public static double[] solveLeastSquares(double[] srcX, double[] srcY, double[] dstU, double[] dstV, int n) {
        if (n < 4) return null;

        double mx = 0, my = 0, mu = 0, mv = 0;
        for (int k = 0; k < n; k++) {
            mx += srcX[k];
            my += srcY[k];
            mu += dstU[k];
            mv += dstV[k];
        }
        mx /= n; my /= n; mu /= n; mv /= n;

        double distSrc = 0, distDst = 0;
        for (int k = 0; k < n; k++) {
            distSrc += Math.hypot(srcX[k] - mx, srcY[k] - my);
            distDst += Math.hypot(dstU[k] - mu, dstV[k] - mv);
        }
        double sSrc = (distSrc > 1e-9) ? (Math.sqrt(2.0) * n / distSrc) : 1.0;
        double sDst = (distDst > 1e-9) ? (Math.sqrt(2.0) * n / distDst) : 1.0;

        // Solve 8x8 normal equations (At * A) h = At * b with h_8 = 1 in normalized coordinates
        double[][] ata = new double[8][9];
        double[] row1 = new double[8];
        double[] row2 = new double[8];

        for (int k = 0; k < n; k++) {
            double x = (srcX[k] - mx) * sSrc;
            double y = (srcY[k] - my) * sSrc;
            double u = (dstU[k] - mu) * sDst;
            double v = (dstV[k] - mv) * sDst;

            row1[0] = x; row1[1] = y; row1[2] = 1.0;
            row1[3] = 0; row1[4] = 0; row1[5] = 0;
            row1[6] = -u * x; row1[7] = -u * y;
            accumulateNormal(ata, row1, u);

            row2[0] = 0; row2[1] = 0; row2[2] = 0;
            row2[3] = x; row2[4] = y; row2[5] = 1.0;
            row2[6] = -v * x; row2[7] = -v * y;
            accumulateNormal(ata, row2, v);
        }

        double[] hNorm8 = solveLinear8x8(ata);
        if (hNorm8 == null) return null;

        double[] Hn = new double[] {
            hNorm8[0], hNorm8[1], hNorm8[2],
            hNorm8[3], hNorm8[4], hNorm8[5],
            hNorm8[6], hNorm8[7], 1.0
        };

        // Denormalize: H = T_dst^-1 * Hn * T_src
        double[] Tsrc = new double[] {
            sSrc, 0, -sSrc * mx,
            0, sSrc, -sSrc * my,
            0, 0, 1.0
        };
        double[] TdstInv = new double[] {
            1.0 / sDst, 0, mu,
            0, 1.0 / sDst, mv,
            0, 0, 1.0
        };
        double[] tmp = mul3x3(Hn, Tsrc);
        double[] H = mul3x3(TdstInv, tmp);
        if (Math.abs(H[8]) > 1e-12) {
            double inv = 1.0 / H[8];
            for (int i = 0; i < 9; i++) H[i] *= inv;
        }
        return H;
    }

    /**
     * RANSAC planar homography fit favoring the main central projection plane,
     * followed by least-squares refinement over all coplanar inliers.
     */
    public static double[] fitRansac(
            double[] allX, double[] allY, double[] allU, double[] allV,
            boolean[] seedMask, int count, double inlierThreshPx) {
        if (count < 4) return null;

        int[] seedIdx = new int[count];
        int seedCount = 0;
        for (int i = 0; i < count; i++) {
            if (seedMask == null || seedMask[i]) seedIdx[seedCount++] = i;
        }
        if (seedCount < 4) {
            seedCount = count;
            for (int i = 0; i < count; i++) seedIdx[i] = i;
        }

        Random rng = new Random(0x5A17C0DEL);
        double[] sx = new double[4];
        double[] sy = new double[4];
        double[] du = new double[4];
        double[] dv = new double[4];
        double[] uv = new double[2];

        double[] bestH = null;
        int bestInliers = -1;
        double bestErrSum = Double.MAX_VALUE;

        int iterations = Math.min(140, Math.max(45, seedCount * 6));
        for (int it = 0; it < iterations; it++) {
            int i0 = seedIdx[rng.nextInt(seedCount)];
            int i1 = seedIdx[rng.nextInt(seedCount)];
            int i2 = seedIdx[rng.nextInt(seedCount)];
            int i3 = seedIdx[rng.nextInt(seedCount)];
            if (i0 == i1 || i0 == i2 || i0 == i3 || i1 == i2 || i1 == i3 || i2 == i3) continue;

            sx[0] = allX[i0]; sy[0] = allY[i0]; du[0] = allU[i0]; dv[0] = allV[i0];
            sx[1] = allX[i1]; sy[1] = allY[i1]; du[1] = allU[i1]; dv[1] = allV[i1];
            sx[2] = allX[i2]; sy[2] = allY[i2]; du[2] = allU[i2]; dv[2] = allV[i2];
            sx[3] = allX[i3]; sy[3] = allY[i3]; du[3] = allU[i3]; dv[3] = allV[i3];

            // Reject degenerate collinear 4-tuples
            if (isCollinear(sx[0], sy[0], sx[1], sy[1], sx[2], sy[2]) ||
                isCollinear(sx[0], sy[0], sx[1], sy[1], sx[3], sy[3])) {
                continue;
            }

            double[] H = solveLeastSquares(sx, sy, du, dv, 4);
            if (H == null) continue;

            int inliers = 0;
            double errSum = 0;
            // Score strictly over central seed region first + global support
            for (int k = 0; k < count; k++) {
                project(H, allX[k], allY[k], uv);
                double err = Math.hypot(uv[0] - allU[k], uv[1] - allV[k]);
                if (err <= inlierThreshPx) {
                    inliers += (seedMask != null && seedMask[k]) ? 2 : 1;
                    errSum += err;
                }
            }
            if (inliers > bestInliers || (inliers == bestInliers && errSum < bestErrSum)) {
                bestInliers = inliers;
                bestErrSum = errSum;
                bestH = H;
            }
        }

        if (bestH == null) {
            return solveLeastSquares(allX, allY, allU, allV, count);
        }

        // Refine over all inliers within threshold
        double[] rx = new double[count];
        double[] ry = new double[count];
        double[] ru = new double[count];
        double[] rv = new double[count];
        int rn = 0;
        for (int k = 0; k < count; k++) {
            project(bestH, allX[k], allY[k], uv);
            if (Math.hypot(uv[0] - allU[k], uv[1] - allV[k]) <= inlierThreshPx) {
                rx[rn] = allX[k];
                ry[rn] = allY[k];
                ru[rn] = allU[k];
                rv[rn] = allV[k];
                rn++;
            }
        }
        if (rn >= 4) {
            double[] refined = solveLeastSquares(rx, ry, ru, rv, rn);
            if (refined != null) return refined;
        }
        return bestH;
    }

    private static boolean isCollinear(double x1, double y1, double x2, double y2, double x3, double y3) {
        double area2 = Math.abs((x2 - x1) * (y3 - y1) - (y2 - y1) * (x3 - x1));
        return area2 < 5000.0;
    }

    private static void accumulateNormal(double[][] ata, double[] row, double rhs) {
        for (int r = 0; r < 8; r++) {
            double vr = row[r];
            if (vr == 0.0) continue;
            for (int c = r; c < 8; c++) {
                ata[r][c] += vr * row[c];
            }
            ata[r][8] += vr * rhs;
        }
    }

    private static double[] solveLinear8x8(double[][] m) {
        for (int r = 0; r < 8; r++) {
            for (int c = 0; c < r; c++) {
                m[r][c] = m[c][r];
            }
        }
        for (int col = 0; col < 8; col++) {
            int pivot = col;
            double maxAbs = Math.abs(m[col][col]);
            for (int r = col + 1; r < 8; r++) {
                double a = Math.abs(m[r][col]);
                if (a > maxAbs) {
                    maxAbs = a;
                    pivot = r;
                }
            }
            if (maxAbs < 1e-11) return null;
            if (pivot != col) {
                double[] tmp = m[col];
                m[col] = m[pivot];
                m[pivot] = tmp;
            }
            double div = m[col][col];
            for (int c = col; c <= 8; c++) m[col][c] /= div;
            for (int r = 0; r < 8; r++) {
                if (r == col) continue;
                double factor = m[r][col];
                if (factor == 0.0) continue;
                for (int c = col; c <= 8; c++) {
                    m[r][c] -= factor * m[col][c];
                }
            }
        }
        double[] x = new double[8];
        for (int i = 0; i < 8; i++) x[i] = m[i][8];
        return x;
    }

    private static double[] mul3x3(double[] A, double[] B) {
        double[] C = new double[9];
        for (int r = 0; r < 3; r++) {
            for (int c = 0; c < 3; c++) {
                C[r * 3 + c] = A[r * 3] * B[c]
                             + A[r * 3 + 1] * B[3 + c]
                             + A[r * 3 + 2] * B[6 + c];
            }
        }
        return C;
    }
}
