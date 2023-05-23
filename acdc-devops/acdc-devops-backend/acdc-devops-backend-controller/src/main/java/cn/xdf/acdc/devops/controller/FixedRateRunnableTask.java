package cn.xdf.acdc.devops.controller;

import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/**
 * Fixed rate task can be scheduled with fixed rate.
 */
public abstract class FixedRateRunnableTask implements Task, Runnable {
    
    public static final int DEFAULT_TASK_SCHEDULE_INTERVAL_IN_SECONDS = 10;
    
    private final TaskScheduler scheduler;
    
    private ScheduledFuture scheduledFuture;
    
    public FixedRateRunnableTask(final TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }
    
    @Override
    public void start() {
        this.start(Duration.ofSeconds(DEFAULT_TASK_SCHEDULE_INTERVAL_IN_SECONDS));
    }
    
    /**
     * Start with fix scheduled rate.
     *
     * @param period fix scheduled rate
     */
    public void start(final Duration period) {
        scheduledFuture = scheduler.scheduleAtFixedRate(this, period);
    }
    
    @Override
    public void stop() {
        scheduledFuture.cancel(false);
    }
}
