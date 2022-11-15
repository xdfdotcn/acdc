package cn.xdf.acdc.devops.informer;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class AbstractInformerTest {

    @Mock
    private TaskScheduler scheduler;

    @Test(expected = TimeoutException.class)
    public void testWaitForInitializedShouldThrowTimeOutExceptionWithTimeout() throws InterruptedException, TimeoutException {
        AbstractInformer<Integer> abstractInformer = fakeAbstractInformer();
        abstractInformer.waitForInitialized(1, TimeUnit.SECONDS);
    }

    @Test
    public void testWaitForInitializedShouldWaitUntilInitialized() throws InterruptedException {
        AbstractInformer<Integer> abstractInformer = fakeAbstractInformer();

        AtomicInteger initializedFlag = new AtomicInteger(1);
        Vector<Exception> unexpectedExceptionHolder = new Vector<>();
        new Thread(() -> {
            try {
                abstractInformer.waitForInitialized(AbstractInformer.DEFAULT_INFORMER_INITIALIZATION_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                unexpectedExceptionHolder.add(e);
            }
            if (initializedFlag.get() == 1) {
                unexpectedExceptionHolder.add(new ExecutionException("not initialized but has released wait lock", null));
            }
        }).start();

        Thread.sleep(1000L);
        initializedFlag.decrementAndGet();
        abstractInformer.run();

        Assert.assertTrue(unexpectedExceptionHolder.isEmpty());
    }

    private AbstractInformer<Integer> fakeAbstractInformer() {
        return new TestInformer(scheduler);
    }

    static class TestInformer extends AbstractInformer<Integer> {

        TestInformer(final TaskScheduler scheduler) {
            super(scheduler);
        }

        @Override
        List<Integer> query() {
            return Lists.newArrayList(1, 2);
        }

        @Override
        Long getKey(final Integer element) {
            return element.longValue();
        }

        @Override
        boolean equals(final Integer e1, final Integer e2) {
            return e1.equals(e2);
        }

        @Override
        boolean isDeleted(final Integer before, final Integer after) {
            return false;
        }

        @Override
        Instant getUpdateTime(final Integer integer) {
            return Instant.now();
        }

    }

}
