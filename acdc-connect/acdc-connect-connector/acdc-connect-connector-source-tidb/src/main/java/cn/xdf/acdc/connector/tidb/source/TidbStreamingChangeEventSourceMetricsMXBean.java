package cn.xdf.acdc.connector.tidb.source;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

import java.util.Map;

public interface TidbStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * Total reader runner count.
     *
     * @return total reader worker count
     */
    int getTotalReaderRunnerCount();

    /**
     * Alive reader runner count.
     *
     * @return alive reader worker count
     */
    int getAliveReaderRunnerCount();

    /**
     * Get committed offset per partition.
     *
     * @return committed offset per partition
     */
    Map<String, Long> getToCommitOffsets();

    /**
     * Get source kafka event position.
     *
     * @return source kafka event position
     */
    Map<String, Long> getSourceKafkaEventPosition();
}
