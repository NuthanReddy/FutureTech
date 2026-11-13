package com.ngantla.personalscreensaver;

import android.app.Service;
import android.content.Intent;
import android.net.Uri;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.os.SystemProperties;
import android.util.Log;

/**
 * Lightweight 750ms property watcher:
 * Whenever HTP's native idle timer opens `com.htp.app.screensaver` in default wallpaper mode,
 * this cleanly dismisses the stock wallpaper activity and presents PersonalSlideshowActivity!
 */
public class HtpSaverWatcherService extends Service {
    private static final String TAG = "PersonalSaverWatcher";
    private final Handler handler = new Handler(Looper.getMainLooper());
    private long lastLaunchMs = 0L;

    private final Runnable checkTask = new Runnable() {
        @Override
        public void run() {
            try {
                String top = SystemProperties.get("sys.lmk_top_app", "");
                if ("com.htp.app.screensaver".equals(top)) {
                    String model = SystemProperties.get("persist.sys.htp.saver.model", "default");
                    long now = System.currentTimeMillis();
                    if ("default".equals(model) && (now - lastLaunchMs) > 2500L) {
                        lastLaunchMs = now;
                        Log.i(TAG, "HTP idle screensaver detected -> switching to PersonalSlideshowActivity!");

                        // 1. Launch our personal 4-photo slideshow seamlessly on top
                        Intent show = new Intent(HtpSaverWatcherService.this, PersonalSlideshowActivity.class);
                        show.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK
                                | Intent.FLAG_ACTIVITY_SINGLE_TOP
                                | Intent.FLAG_ACTIVITY_NO_ANIMATION);
                        startActivity(show);

                        // 2. Tell stock ScreenSaverActivity to finish() itself cleanly behind us
                        //    so a single remote key press returns directly to the user's app/launcher!
                        Intent closeStock = new Intent("android.intent.action.MEDIA_MOUNTED", Uri.parse("file:///Reserve0"));
                        closeStock.setClassName("com.htp.app.screensaver",
                                "com.htp.app.screensaver.broadcast.UsbNotifyBroadCastReceiver");
                        sendBroadcast(closeStock);
                    }
                }
            } catch (Throwable t) {
                Log.w(TAG, "Watcher tick error: " + t.getMessage());
            }
            handler.postDelayed(this, 700L);
        }
    };

    @Override
    public void onCreate() {
        super.onCreate();
        handler.post(checkTask);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        handler.removeCallbacksAndMessages(null);
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}
