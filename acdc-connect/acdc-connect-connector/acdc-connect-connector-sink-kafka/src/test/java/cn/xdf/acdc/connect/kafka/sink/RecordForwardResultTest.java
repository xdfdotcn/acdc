package cn.xdf.acdc.connect.kafka.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class RecordForwardResultTest {

    private RecordForwardResult recordForwardResult;

    @Before
    public void setup() {
        recordForwardResult = new RecordForwardResult(0L);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCancelShouldThrowUnsupportedException() {
        recordForwardResult.cancel(false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsCanceledShouldThrowUnsupportedException() {
        recordForwardResult.isCancelled();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetShouldThrowUnsupportedException() {
        recordForwardResult.get();
    }

    @Test(expected = TimeoutException.class)
    public void testGetWithTimeOutShouldThrowTimeoutException() throws ExecutionException, InterruptedException, TimeoutException {
        recordForwardResult.get(1, TimeUnit.SECONDS);
    }

    @Test(expected = ExecutionException.class)
    public void testGetWithDownExceptionShouldThrowExecutionException() throws ExecutionException, InterruptedException, TimeoutException {
        new Thread(() -> {
            recordForwardResult.done(new RuntimeException("runtime exception!!"));
        }).start();
        recordForwardResult.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testDoneBeforeGet() throws InterruptedException {
        int taskNum = 50;
        ExecutorService threadPool = Executors.newFixedThreadPool(taskNum * 2);

        List<RecordForwardResult> recordForwardResultList = getRecordForwardResults(taskNum);
        CountDownLatch countDownLatch = new CountDownLatch(taskNum);

        Vector<Exception> exceptions = new Vector<>();
        recordForwardResultList.forEach(recordForwardResult -> {

            threadPool.submit(() -> {
                recordForwardResult.done(null);
                countDownLatch.countDown();
            });

            threadPool.submit(() -> {
                try {
                    countDownLatch.await();
                    recordForwardResult.get(1, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException | ConnectException | InterruptedException e) {
                    exceptions.add(e);
                }
            });

        });
        threadPool.shutdown();
        Assert.assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
        log.info(exceptions.toString());
        Assert.assertEquals(0, exceptions.size());
    }

    @Test
    public void testDoneAfterGet() throws InterruptedException {
        int taskNum = 50;
        ExecutorService threadPool = Executors.newFixedThreadPool(taskNum * 2);

        List<RecordForwardResult> recordForwardResultList = getRecordForwardResults(taskNum);
        CountDownLatch countDownLatch = new CountDownLatch(taskNum);

        Vector<Exception> exceptions = new Vector<>();
        recordForwardResultList.forEach(recordForwardResult -> {

            threadPool.submit(() -> {
                try {
                    countDownLatch.await(10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    exceptions.add(e);
                }

                recordForwardResult.done(null);
            });

            threadPool.submit(() -> {
                try {
                    countDownLatch.countDown();
                    recordForwardResult.get(1, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException | ConnectException | InterruptedException e) {
                    exceptions.add(e);
                }
            });
        });
        threadPool.shutdown();
        Assert.assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
        log.info(exceptions.toString());
        Assert.assertEquals(0, exceptions.size());
    }

    @Test
    public void testDoneAndGet() throws InterruptedException {
        int taskNum = 50;
        ExecutorService threadPool = Executors.newFixedThreadPool(taskNum * 2);

        List<RecordForwardResult> recordForwardResultList = getRecordForwardResults(taskNum);

        Vector<Exception> exceptions = new Vector<>();
        recordForwardResultList.forEach(recordForwardResult -> {

            threadPool.submit(() -> {
                recordForwardResult.done(null);
            });

            threadPool.submit(() -> {
                try {
                    recordForwardResult.get(1, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException | ConnectException | InterruptedException e) {
                    exceptions.add(e);
                }
            });
        });
        threadPool.shutdown();
        Assert.assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
        log.info(exceptions.toString());
        Assert.assertEquals(0, exceptions.size());
    }

    private List<RecordForwardResult> getRecordForwardResults(final int taskNum) {
        AtomicLong i = new AtomicLong(0);

        return Arrays.stream(new RecordForwardResult[taskNum])
                .map(empty -> new RecordForwardResult(i.getAndIncrement()))
                .collect(Collectors.toList());
    }

}
