package com.ngantla.personalscreensaver;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Color;
import android.os.Handler;
import android.os.Looper;
import android.view.Gravity;
import android.widget.FrameLayout;
import android.widget.ImageView;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reusable zero-bloat 1080p crossfading slideshow view backed by /sdcard/Screensaver
 * (falling back to /Reserve0/screensaver).
 */
public final class SlideshowController {
    private static final long SLIDE_INTERVAL_MS = 15000L;
    private static final long FADE_DURATION_MS  = 900L;

    private final FrameLayout root;
    private final ImageView frontView;
    private final ImageView backView;
    private final Handler handler = new Handler(Looper.getMainLooper());
    private final List<String> photoPaths = new ArrayList<>();

    private int currentIndex = 0;
    private boolean frontActive = true;
    private boolean running = false;
    private Bitmap bmpA;
    private Bitmap bmpB;

    private final Runnable advanceTask = new Runnable() {
        @Override
        public void run() {
            if (!running || photoPaths.size() <= 1) return;
            currentIndex = (currentIndex + 1) % photoPaths.size();
            showSlide(currentIndex, true);
            handler.postDelayed(this, SLIDE_INTERVAL_MS);
        }
    };

    public SlideshowController(Context ctx) {
        root = new FrameLayout(ctx);
        root.setBackgroundColor(Color.BLACK);
        root.setKeepScreenOn(true);

        FrameLayout.LayoutParams lp = new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT,
                Gravity.CENTER);

        backView = new ImageView(ctx);
        backView.setScaleType(ImageView.ScaleType.CENTER_CROP);
        backView.setAlpha(0f);
        root.addView(backView, lp);

        frontView = new ImageView(ctx);
        frontView.setScaleType(ImageView.ScaleType.CENTER_CROP);
        frontView.setAlpha(1f);
        root.addView(frontView, lp);
    }

    public FrameLayout getView() {
        return root;
    }

    public void start() {
        if (running) return;
        loadPhotoList();
        if (photoPaths.isEmpty()) return;
        running = true;
        currentIndex = 0;
        showSlide(0, false);
        if (photoPaths.size() > 1) {
            handler.postDelayed(advanceTask, SLIDE_INTERVAL_MS);
        }
    }

    public void stop() {
        running = false;
        handler.removeCallbacksAndMessages(null);
        frontView.setImageDrawable(null);
        backView.setImageDrawable(null);
        if (bmpA != null) { bmpA.recycle(); bmpA = null; }
        if (bmpB != null) { bmpB.recycle(); bmpB = null; }
    }

    private void loadPhotoList() {
        photoPaths.clear();
        String[] dirs = { "/sdcard/Screensaver", "/Reserve0/screensaver", "/Reserve0/ss" };
        for (String d : dirs) {
            File dir = new File(d);
            File[] files = dir.listFiles();
            if (files == null || files.length == 0) continue;
            Arrays.sort(files);
            for (File f : files) {
                String n = f.getName().toLowerCase();
                if (f.isFile() && f.length() > 1024
                        && (n.endsWith(".jpg") || n.endsWith(".jpeg") || n.endsWith(".png") || n.endsWith(".webp"))) {
                    photoPaths.add(f.getAbsolutePath());
                }
            }
            if (!photoPaths.isEmpty()) break;
        }
    }

    private void showSlide(int idx, boolean animate) {
        Bitmap decoded = decodeScaled1080p(photoPaths.get(idx));
        if (decoded == null) return;

        if (!animate) {
            if (bmpA != null && bmpA != decoded) bmpA.recycle();
            bmpA = decoded;
            frontView.setImageBitmap(bmpA);
            frontView.setAlpha(1f);
            backView.setAlpha(0f);
            frontActive = true;
            return;
        }

        final ImageView incoming = frontActive ? backView : frontView;
        final ImageView outgoing = frontActive ? frontView : backView;
        if (frontActive) {
            if (bmpB != null && bmpB != decoded) bmpB.recycle();
            bmpB = decoded;
        } else {
            if (bmpA != null && bmpA != decoded) bmpA.recycle();
            bmpA = decoded;
        }
        frontActive = !frontActive;

        incoming.animate().cancel();
        outgoing.animate().cancel();
        incoming.setImageBitmap(decoded);
        incoming.setAlpha(0f);
        incoming.bringToFront();
        incoming.animate().alpha(1f).setDuration(FADE_DURATION_MS).start();
        outgoing.animate().alpha(0f).setDuration(FADE_DURATION_MS).start();
    }

    private static Bitmap decodeScaled1080p(String path) {
        try {
            BitmapFactory.Options bounds = new BitmapFactory.Options();
            bounds.inJustDecodeBounds = true;
            BitmapFactory.decodeFile(path, bounds);
            int sample = 1;
            while (bounds.outWidth / (sample * 2) >= 1920 && bounds.outHeight / (sample * 2) >= 1080) {
                sample *= 2;
            }
            BitmapFactory.Options opts = new BitmapFactory.Options();
            opts.inSampleSize = sample;
            opts.inPreferredConfig = Bitmap.Config.ARGB_8888;
            return BitmapFactory.decodeFile(path, opts);
        } catch (Throwable t) {
            return null;
        }
    }
}
