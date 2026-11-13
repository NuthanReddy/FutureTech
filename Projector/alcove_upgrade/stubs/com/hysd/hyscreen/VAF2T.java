package com.hysd.hyscreen;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

public class VAF2T extends Service {
    public float[] result_angle;

    @Override
    public IBinder onBind(Intent intent) { return null; }

    @Override
    public void onCreate() { super.onCreate(); }

    @Override
    public void onDestroy() { super.onDestroy(); }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return super.onStartCommand(intent, flags, startId);
    }

    public int workfinished() {
        return 0;
    }

    public int motorthreadstop(int code) {
        return 0;
    }

    public int motor_stop() {
        return 0;
    }

    public void delay(int ms) {
    }
}
