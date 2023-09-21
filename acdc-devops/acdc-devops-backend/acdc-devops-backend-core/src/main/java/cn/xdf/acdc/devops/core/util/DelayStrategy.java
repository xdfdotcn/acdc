package cn.xdf.acdc.devops.core.util;

/**
 * 定时任务,延迟策略常量类.
 */
public class DelayStrategy {
    
    public static final long MAX_TIME_INTERVAL = 4320_000L;
    
    private static final long BASE_TIME_INTERVAL = 60_000L;
    
    private static final long BACKOFF_MULTIPLIER = 2L;
    
    private volatile long currentTimeInterval = BASE_TIME_INTERVAL;
    
    private volatile long lastTime;
    
    public DelayStrategy() {
    }
    
    public DelayStrategy(final long timeIntervalInMillis) {
        currentTimeInterval = timeIntervalInMillis;
    }
    
    /**
     * 时间阀值是否触发.
     *
     * @return 是否触发
     */
    public boolean isReached() {
        long now = System.currentTimeMillis();
        if (lastTime == 0) {
            lastTime = now;
            return true;
        }
        
        long timeInterval = now - lastTime;
        if (timeInterval < currentTimeInterval) {
            return false;
        }
        
        long nextTimeInterval = currentTimeInterval * BACKOFF_MULTIPLIER;
        currentTimeInterval = Math.min(nextTimeInterval, MAX_TIME_INTERVAL);
        lastTime = now;
        return true;
    }
    
}
