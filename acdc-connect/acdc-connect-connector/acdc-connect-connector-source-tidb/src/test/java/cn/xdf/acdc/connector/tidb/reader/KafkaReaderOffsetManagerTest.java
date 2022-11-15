package cn.xdf.acdc.connector.tidb.reader;

import cn.xdf.acdc.connector.tidb.util.DelayStrategy;
import cn.xdf.acdc.connector.tidb.util.ReflectionUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaReaderOffsetManagerTest {

    private final KafkaReaderOffsetManager offsetManager = new KafkaReaderOffsetManager();

    @Test
    public void testInitShouldThrowNullPointExceptionWithNullInput() {
        Assert.assertThrows(NullPointerException.class, () -> offsetManager.init(null));
    }

    @Test
    public void testInitShouldFillingWithParam() throws NoSuchFieldException, IllegalAccessException {
        Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> toCommitPartitionOffsetsBuffer =
                (Map) ReflectionUtils.getLimitedAccessField(offsetManager, "toCommitPartitionOffsetsBuffer");
        Map<Integer, Long> toCommitPartitionOffsets =
                (Map) ReflectionUtils.getLimitedAccessField(offsetManager, "toCommitPartitionOffsets");

        Map<Integer, Long> beginOffset = new HashMap<>();
        offsetManager.init(beginOffset);
        Assert.assertEquals(0, toCommitPartitionOffsetsBuffer.size());
        Assert.assertEquals(0, toCommitPartitionOffsets.size());
        beginOffset.put(0, 11110L);
        beginOffset.put(1, 11111L);
        beginOffset.put(2, 11112L);
        offsetManager.init(beginOffset);
        Assert.assertEquals(3, toCommitPartitionOffsetsBuffer.size());
        Assert.assertEquals(3, toCommitPartitionOffsets.size());
    }

    @Test
    public void testAddShouldPutDataIntoBufferWithBufferNotOverload() throws NoSuchFieldException, IllegalAccessException {
        Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> toCommitPartitionOffsetsBuffer = initBuffer(0);
        Set<Integer> eventIdSet = new HashSet<Integer>() {
            {
                add(0);
            }
        };
        offsetManager.add(0, 111110L, eventIdSet);
        Assert.assertEquals(eventIdSet, toCommitPartitionOffsetsBuffer.get(0).get(111110L));
    }

    @Test
    public void testBufferMaybeOverload() throws NoSuchFieldException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        initBuffer(0);
        initToCommit(0, 0L);
        Set<Integer> eventIdSet = new HashSet<Integer>() {
            {
                add(0);
            }
        };
        for (long i = 0; i <= 50000; i++) {
            offsetManager.add(0, i, eventIdSet);
        }
        Assert.assertTrue((Boolean) ReflectionUtils.getLimitedAccessMethodExecuteResultWithoutArgs(offsetManager, "bufferMaybeOverload"));
        offsetManager.markTicdcEventAsDone(0, 0, 0);
        Assert.assertFalse((Boolean) ReflectionUtils.getLimitedAccessMethodExecuteResultWithoutArgs(offsetManager, "bufferMaybeOverload"));
    }

    @Test
    public void testIfNeedCommit() throws NoSuchFieldException, IllegalAccessException {
        Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> buffer = initBuffer(0);
        initToCommit(0, 111110L);
        Set<Integer> eventIdSet = new HashSet<Integer>() {
            {
                add(0);
            }
        };
        buffer.get(0).putIfAbsent(111110L, eventIdSet);
        Assert.assertFalse(offsetManager.ifNeedCommit());
        buffer.get(0).get(111110L).remove(0);
        Assert.assertTrue(offsetManager.ifNeedCommit());
    }

    @Test
    public void testClearShouldThrowNullPointExceptionWithNullInput() {
        Assert.assertThrows(NullPointerException.class, () -> offsetManager.clear(null));
    }

    @Test
    public void testClearShouldClearInputPartition() throws NoSuchFieldException, IllegalAccessException {
        initToCommit(0, 100L);
        Map<Integer, Long> toCommit = initToCommit(1, 101L);
        initBuffer(0);
        Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> buffer = initBuffer(1);
        offsetManager.clear(Lists.newArrayList(0));
        Assert.assertEquals(1, buffer.size());
        Assert.assertEquals(1, buffer.keySet().toArray()[0]);
        Assert.assertEquals(1, toCommit.size());
        Assert.assertEquals(1, toCommit.keySet().toArray()[0]);
        offsetManager.clear(Lists.newArrayList(1));
        Assert.assertEquals(0, buffer.size());
        Assert.assertEquals(0, toCommit.size());
    }

    private Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> initBuffer(int partition) throws NoSuchFieldException, IllegalAccessException {
        Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> toCommitPartitionOffsetsBuffer =
                (Map) ReflectionUtils.getLimitedAccessField(offsetManager, "toCommitPartitionOffsetsBuffer");
        toCommitPartitionOffsetsBuffer.put(partition, new ConcurrentSkipListMap<>());
        return toCommitPartitionOffsetsBuffer;
    }

    private Map<Integer, Long> initToCommit(int partition, long offset) throws NoSuchFieldException, IllegalAccessException {
        Map<Integer, Long> toCommitPartitionOffsets =
                (Map) ReflectionUtils.getLimitedAccessField(offsetManager, "toCommitPartitionOffsets");
        toCommitPartitionOffsets.put(partition, offset);
        return toCommitPartitionOffsets;
    }

    @Test
    public void testGetDelayStrategyShouldReturnSameInstanceWithInSameThread() throws InterruptedException {
        offsetManager.getDelayStrategy();
        AtomicReference<DelayStrategy> delayStrategy1 = new AtomicReference<>();
        AtomicReference<DelayStrategy> delayStrategy2 = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            delayStrategy1.set(offsetManager.getDelayStrategy());
            delayStrategy2.set(offsetManager.getDelayStrategy());
        });
        t1.start();
        t1.join();

        Assert.assertEquals(delayStrategy1.get(), delayStrategy2.get());
    }

    @Test
    public void testGetDelayStrategyShouldReturnDiffInstanceWithDiffThread() throws InterruptedException {
        offsetManager.getDelayStrategy();
        AtomicReference<DelayStrategy> delayStrategy = new AtomicReference<>();
        AtomicReference<DelayStrategy> anotherDelayStrategy = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            delayStrategy.set(offsetManager.getDelayStrategy());
        });
        Thread t2 = new Thread(() -> {
            anotherDelayStrategy.set(offsetManager.getDelayStrategy());
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        Assert.assertNotEquals(delayStrategy.get(), anotherDelayStrategy.get());
    }
}
