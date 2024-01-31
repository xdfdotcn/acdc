package cn.xdf.acdc.devops.informer;

import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

/**
 * Abstract informer provide common process capabilities.
 *
 * @param <E> element
 */
public abstract class AbstractFixedRateRunnableInformer<E> extends AbstractInformer<E> {
    
    public static final int DEFAULT_TASK_SCHEDULE_INTERVAL_IN_SECONDS = 10;
    
    private final TaskScheduler scheduler;
    
    private ScheduledFuture scheduledFuture;
    
    public AbstractFixedRateRunnableInformer(final TaskScheduler scheduler) {
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
    
    /**
     * Register add call back.
     *
     * @param callback call back
     * @return this informer
     */
    @Override
    public AbstractFixedRateRunnableInformer<E> whenAdd(final Consumer<E> callback) {
        super.whenAdd(callback);
        return this;
    }
    
    /**
     * Register delete call back.
     *
     * @param callback call back
     * @return this informer
     */
    @Override
    public AbstractFixedRateRunnableInformer<E> whenDelete(final Consumer<E> callback) {
        super.whenDelete(callback);
        return this;
    }
    
    /**
     * Register update call back.
     *
     * @param callback call back
     * @return this informer
     */
    @Override
    public AbstractFixedRateRunnableInformer<E> whenUpdate(final Consumer<E> callback) {
        super.whenUpdate(callback);
        return this;
    }
}
