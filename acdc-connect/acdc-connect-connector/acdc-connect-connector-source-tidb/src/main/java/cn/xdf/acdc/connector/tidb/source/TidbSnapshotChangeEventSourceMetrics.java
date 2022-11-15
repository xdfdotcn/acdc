package cn.xdf.acdc.connector.tidb.source;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class TidbSnapshotChangeEventSourceMetrics extends SnapshotChangeEventSourceMetrics implements SnapshotChangeEventSourceMetricsMXBean {

    public TidbSnapshotChangeEventSourceMetrics(final CdcSourceTaskContext taskContext, final ChangeEventQueueMetrics changeEventQueueMetrics,
                                                final EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }
}
