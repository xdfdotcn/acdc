package cn.xdf.acdc.devops.controller;

import org.springframework.context.SmartLifecycle;

/**
 * A controller can coordinate multi components to provide capabilities.
 */
public abstract class Controller implements SmartLifecycle {
    
    private volatile boolean isRunning;
    
    @Override
    public void start() {
        doStart();
        isRunning = true;
    }
    
    @Override
    public void stop() {
        doStop();
        isRunning = false;
    }
    
    @Override
    public boolean isRunning() {
        return isRunning;
    }
    
    abstract void doStart();
    
    abstract void doStop();
}
