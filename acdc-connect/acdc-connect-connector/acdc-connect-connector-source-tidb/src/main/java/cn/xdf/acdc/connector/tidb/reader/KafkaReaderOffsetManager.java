package cn.xdf.acdc.connector.tidb.reader;

import cn.xdf.acdc.connector.tidb.util.DelayStrategy;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class KafkaReaderOffsetManager {

    private static final int MAX_BUFFER_SIZE = 50_000;

    private static final int BUFFER_RELEASE_TIMEOUT_MILLISECONDS = 30_000;

    private final ThreadLocal<DelayStrategy> localDelayStrategy = ThreadLocal.withInitial(
        () -> DelayStrategy.exponentialWithTimeoutException(BUFFER_RELEASE_TIMEOUT_MILLISECONDS)
    );

    /**
     * Poll origin topic record and keep the dealing record's offsets in this buffer,
     * once the record is dealt ,do commit the offset to kafka and release the buffer.
     */
    private final Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> toCommitPartitionOffsetsBuffer = new ConcurrentHashMap<>();

    /**
     * Last committed offset per partition.
     */
    private final Map<Integer, Long> toCommitPartitionOffsets = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Init offset manager with begin offset.
     *
     * @param beginOffset begin offset
     */
    public void init(final Map<Integer, Long> beginOffset) {
        Preconditions.checkNotNull(beginOffset, "Begin offset should not be null.");
        toCommitPartitionOffsets.putAll(beginOffset);
        Map<Integer, ConcurrentSkipListMap<Long, Set<Integer>>> newPartitionOffsetsBuffer = beginOffset.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new ConcurrentSkipListMap<>()));
        toCommitPartitionOffsetsBuffer.putAll(newPartitionOffsetsBuffer);
    }

    /**
     * Add new events to offset manager.
     *
     * @param partition  partition
     * @param offset     offset
     * @param eventIdSet eventIds
     * @throws RetriableException retriable exception
     */
    public void add(final Integer partition, final Long offset, final Set<Integer> eventIdSet) throws RetriableException {
        DelayStrategy delayStrategy = getDelayStrategy();
        delayStrategy.sleepWhenNotTimeout(this::bufferMaybeOverload);
        toCommitPartitionOffsetsBuffer.get(partition).putIfAbsent(offset, eventIdSet);
    }

    /**
     * Get delay strategy.
     *
     * @return DelayStrategy delay strategy
     */
    protected DelayStrategy getDelayStrategy() {
        return localDelayStrategy.get();
    }

    private boolean bufferMaybeOverload() {
        if (getToCommitPartitionOffsetsBufferSize() > MAX_BUFFER_SIZE) {
            ifNeedCommit();
        }
        return getToCommitPartitionOffsetsBufferSize() > MAX_BUFFER_SIZE;
    }

    /**
     * One message may contain multi table data records.
     *
     * @param partition    partition
     * @param offset       offset
     * @param orderInBatch event order in one batch
     */
    public void markTicdcEventAsDone(final int partition, final long offset, final int orderInBatch) {
        ConcurrentSkipListMap<Long, Set<Integer>> offsetsBuffer = toCommitPartitionOffsetsBuffer.get(partition);
        if (Objects.isNull(offsetsBuffer)) {
            log.warn("Partition: {}, offsets buffer is null, usually consumer is rebalancing.", partition);
            return;
        }
        offsetsBuffer.computeIfPresent(offset, (k, v) -> {
            v.remove(orderInBatch);
            return v;
        });
    }

    /**
     * Get to commit offsets.
     *
     * @return to commit offsets map
     */
    public Map<Integer, Long> getToCommitOffsets() {
        return toCommitPartitionOffsets;
    }

    /**
     * Is need commit or not.
     *
     * @return is need commit
     */
    public boolean ifNeedCommit() {
        if (lock.tryLock()) {
            AtomicBoolean ifNeedCommit = new AtomicBoolean(false);
            try {
                toCommitPartitionOffsetsBuffer.forEach((partition, partitionBuffer) -> {
                    catchUpKafkaProduceSuccessOffsetAndReleaseBuffer(ifNeedCommit, partition, partitionBuffer);
                });
            } finally {
                lock.unlock();
                return ifNeedCommit.get();
            }
        }
        return false;
    }

    private void catchUpKafkaProduceSuccessOffsetAndReleaseBuffer(final AtomicBoolean isCompact, final Integer partition,
                                                                  final ConcurrentSkipListMap<Long, Set<Integer>> partitionBuffer) {
        long toCommitOffset = toCommitPartitionOffsets.get(partition);
        long cursor = toCommitOffset - 1;
        if (!partitionBuffer.isEmpty()) {
            while (partitionBuffer.containsKey(++cursor) && partitionBuffer.get(cursor).isEmpty()) {
                while (!partitionBuffer.isEmpty() && partitionBuffer.firstEntry().getKey() <= cursor) {
                    partitionBuffer.pollFirstEntry();
                }
            }
            toCommitPartitionOffsets.put(partition, cursor);
            if (cursor > toCommitOffset) {
                isCompact.set(true);
            }
        }
    }

    /**
     * Clear reader buffer data.
     *
     * @param revokedPartitions revoked partitions
     */
    public void clear(final List<Integer> revokedPartitions) {
        Preconditions.checkNotNull(revokedPartitions);
        revokedPartitions.forEach(partition -> {
            toCommitPartitionOffsets.remove(partition);
            toCommitPartitionOffsetsBuffer.remove(partition);
        });
    }

    /**
     * Get to commit partition offsets buffer size.
     *
     * @return to commit partition offsets buffer size
     */
    private int getToCommitPartitionOffsetsBufferSize() {
        int size = 0;
        for (Map<Long, Set<Integer>> offsetBuffer : toCommitPartitionOffsetsBuffer.values()) {
            size += offsetBuffer.size();
        }
        return size;
    }

}
