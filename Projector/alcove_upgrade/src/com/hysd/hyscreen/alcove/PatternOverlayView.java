package com.hysd.hyscreen.alcove;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.view.View;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Constant-50%-flux 3-frame complementary checkerboard renderer for 1920x1080.
 * Keeps total projected light constant across all frames so UVC camera Auto-Exposure
 * never shifts gain or shutter, while canceling both ambient light and room interreflection.
 */
public final class PatternOverlayView extends View {
    public static final int PROJ_W = 1920;
    public static final int PROJ_H = 1080;

    // 20 x 12 checkerboard tiles -> 96 x 90 projector px per tile (~24 x 22 camera px)
    // Interior saddle corners: c in [1..19] at X = 96*c, r in [1..11] at Y = 90*r (209 corners!)
    public static final int TILE_COLS = 20;
    public static final int TILE_ROWS = 12;
    public static final int TILE_W = PROJ_W / TILE_COLS; // 96 px
    public static final int TILE_H = PROJ_H / TILE_ROWS; // 90 px

    // Origin coded tile (tx=9, ty=5 -> X in [864..960], Y in [450..540]; plus asymmetric +X tag at tx=10, ty=5)
    public static final int ORIGIN_CORNER_COL = 10; // X = 960
    public static final int ORIGIN_CORNER_ROW = 6;  // Y = 540

    public static final int ILLUM_HIGH = Color.rgb(255, 255, 255);
    public static final int ILLUM_LOW  = Color.rgb(0, 0, 0);

    public static final int MODE_CHECKER_POS = 0;
    public static final int MODE_CHECKER_NEG = 1;
    public static final int MODE_ORIGIN_TAG  = 2;
    public static final int MODE_HUD_CONFIRM = 3;

    private volatile int mode = MODE_CHECKER_POS;
    private volatile CountDownLatch drawLatch;
    private volatile String hudMessage = "";
    private final int[] confirmQuad = new int[8];

    private final Paint lightPaint = new Paint();
    private final Paint borderPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
    private final Paint textPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
    private final Paint badgePaint = new Paint(Paint.ANTI_ALIAS_FLAG);

    public PatternOverlayView(Context context) {
        super(context);
        lightPaint.setColor(ILLUM_HIGH);
        lightPaint.setStyle(Paint.Style.FILL);

        borderPaint.setColor(0xFF00E676);
        borderPaint.setStyle(Paint.Style.STROKE);
        borderPaint.setStrokeWidth(6f);

        badgePaint.setColor(0xCC101820);
        badgePaint.setStyle(Paint.Style.FILL);

        textPaint.setColor(Color.WHITE);
        textPaint.setTextSize(32f);
    }

    public static float cornerProjX(int c) {
        return c * TILE_W;
    }

    public static float cornerProjY(int r) {
        return r * TILE_H;
    }

    public void setPatternAndAwaitDraw(final int nextMode, long timeoutMs) {
        final CountDownLatch latch = new CountDownLatch(1);
        this.drawLatch = latch;
        this.mode = nextMode;
        post(new Runnable() {
            @Override
            public void run() {
                invalidate();
            }
        });
        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public void showConfirmHud(int[] insets, String msg) {
        if (insets != null && insets.length == 8) {
            System.arraycopy(insets, 0, confirmQuad, 0, 8);
        }
        this.hudMessage = (msg != null) ? msg : "";
        setPatternAndAwaitDraw(MODE_HUD_CONFIRM, 150);
    }

    @Override
    protected void onDraw(Canvas canvas) {
        final int w = getWidth() > 0 ? getWidth() : PROJ_W;
        final int h = getHeight() > 0 ? getHeight() : PROJ_H;
        final float sx = w / (float) PROJ_W;
        final float sy = h / (float) PROJ_H;

        final int m = this.mode;
        if (m == MODE_CHECKER_POS || m == MODE_CHECKER_NEG || m == MODE_ORIGIN_TAG) {
            canvas.drawColor(ILLUM_LOW);
            boolean invertAll = (m == MODE_CHECKER_NEG);
            for (int ty = 0; ty < TILE_ROWS; ty++) {
                float y0 = ty * TILE_H * sy;
                float y1 = (ty + 1) * TILE_H * sy;
                for (int tx = 0; tx < TILE_COLS; tx++) {
                    boolean bright = (((tx + ty) & 1) == 0) ^ invertAll;
                    // In MODE_ORIGIN_TAG, flip the 3 L-shaped tiles around corner (10,6)=(960,540):
                    //   (tx=9, ty=5) [top-left of (960,540)],
                    //   (tx=10, ty=5) [top-right of (960,540) -> +X],
                    //   (tx=9, ty=6) [bottom-left of (960,540) -> +Y],
                    // while keeping (tx=10, ty=6) unflipped so both origin AND +X/+Y axes are uniquely tagged!
                    if (m == MODE_ORIGIN_TAG) {
                        if ((tx == 9 && ty == 5) || (tx == 10 && ty == 5) || (tx == 9 && ty == 6)) {
                            bright = !bright;
                        }
                    }
                    if (bright) {
                        float x0 = tx * TILE_W * sx;
                        float x1 = (tx + 1) * TILE_W * sx;
                        canvas.drawRect(x0, y0, x1, y1, lightPaint);
                    }
                }
            }
        } else if (m == MODE_HUD_CONFIRM) {
            canvas.drawColor(0x00000000);
            float pad = 6f;
            canvas.drawRect(pad, pad, w - pad, h - pad, borderPaint);
            if (!hudMessage.isEmpty()) {
                float tw = textPaint.measureText(hudMessage) + 48f;
                float bx = (w - tw) * 0.5f;
                float by = h * 0.5f - 36f;
                canvas.drawRoundRect(bx, by, bx + tw, by + 72f, 16f, 16f, badgePaint);
                canvas.drawText(hudMessage, bx + 24f, by + 47f, textPaint);
            }
        }

        final CountDownLatch latch = this.drawLatch;
        if (latch != null) {
            this.drawLatch = null;
            postOnAnimation(new Runnable() {
                @Override
                public void run() {
                    latch.countDown();
                }
            });
        }
    }
}
