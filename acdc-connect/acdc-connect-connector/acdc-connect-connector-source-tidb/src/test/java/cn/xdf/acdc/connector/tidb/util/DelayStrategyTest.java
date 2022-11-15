package cn.xdf.acdc.connector.tidb.util;

import org.junit.Assert;
import org.junit.Test;

public class DelayStrategyTest {

    @Test(expected = org.apache.kafka.common.errors.TimeoutException.class)
    public void testSleepWhenNotTimeoutShouldThrowExceptionWithTimeout() {
        DelayStrategy delayStrategy = DelayStrategy.exponentialWithTimeoutException(2000);
        delayStrategy.sleepWhenNotTimeout(() -> true);
    }

    @Test
    public void testSleepWhenNotTimeoutShouldReturn() {
        DelayStrategy delayStrategy = DelayStrategy.exponentialWithTimeoutException(2000);
        long startTime = System.currentTimeMillis();
        delayStrategy.sleepWhenNotTimeout(() -> {
            long interval = System.currentTimeMillis() - startTime;
            return interval <= 1000;
        });
        //reached
        Assert.assertTrue(true);
    }
}
