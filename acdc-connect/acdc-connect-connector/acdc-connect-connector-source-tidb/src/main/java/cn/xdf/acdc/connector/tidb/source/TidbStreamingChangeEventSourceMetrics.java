package cn.xdf.acdc.connector.tidb.source;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

import java.util.Map;
import java.util.stream.Collectors;

public class TidbStreamingChangeEventSourceMetrics extends StreamingChangeEventSourceMetrics implements TidbStreamingChangeEventSourceMetricsMXBean {

    private static final String PARTITION_PREFIX = "PARTITION-";

    private final TidbSourceTaskContext taskContext;

    public TidbStreamingChangeEventSourceMetrics(final TidbSourceTaskContext taskContext, final ChangeEventQueueMetrics changeEventQueueMetrics,
                                                 final EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
        this.taskContext = taskContext;
    }

    @Override
    public int getTotalReaderRunnerCount() {
        return taskContext.getTidbDataReader().getTotalRunnerCount();
    }

    @Override
    public int getAliveReaderRunnerCount() {
        return taskContext.getTidbDataReader().getAliveRunnerCount();
    }

    @Override
    public Map<String, Long> getSourceKafkaEventPosition() {
        Map<Thread, TidbOffsetContext> offsetContextHolder = taskContext.getOffsetContextHolder();
        return offsetContextHolder.values().stream().collect(
            Collectors.toMap(
                context -> PARTITION_PREFIX + context.getOffset().get(TidbOffsetContext.READER_PARTITION),
                context -> (Long) context.getOffset().get(TidbOffsetContext.READER_OFFSET)
            )
        );
    }

    @Override
    public Map<String, Long> getToCommitOffsets() {
        return taskContext.getTidbDataReader().getToCommitOffset().entrySet().stream().collect(
            Collectors.toMap(
                entry -> PARTITION_PREFIX + entry.getKey(),
                entry -> (Long) entry.getValue()
            )
        );
    }

}
