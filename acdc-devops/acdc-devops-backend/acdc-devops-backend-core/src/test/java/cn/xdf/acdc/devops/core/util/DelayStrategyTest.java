package cn.xdf.acdc.devops.core.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DelayStrategyTest {

    private DelayStrategy delayStrategy;

    @Before
    public void setup() {
        delayStrategy = new DelayStrategy();
    }

    @Test
    public void testIsReached() throws InterruptedException {
        Assert.assertEquals(true, delayStrategy.isReached());
        Thread.sleep(1000L);
        Assert.assertEquals(false, delayStrategy.isReached());
        Thread.sleep(1001L);
        Assert.assertEquals(true, delayStrategy.isReached());

        Assert.assertEquals(false, delayStrategy.isReached());
    }
}
