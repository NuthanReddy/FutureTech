package com.ngantla.personalscreensaver;

import android.service.dreams.DreamService;

public class PhotoDreamService extends DreamService {
    private SlideshowController controller;

    @Override
    public void onAttachedToWindow() {
        super.onAttachedToWindow();
        setInteractive(false);
        setFullscreen(true);
        setScreenBright(true);
        controller = new SlideshowController(this);
        setContentView(controller.getView());
    }

    @Override
    public void onDreamingStarted() {
        super.onDreamingStarted();
        if (controller != null) controller.start();
    }

    @Override
    public void onDreamingStopped() {
        if (controller != null) controller.stop();
        super.onDreamingStopped();
    }
}
