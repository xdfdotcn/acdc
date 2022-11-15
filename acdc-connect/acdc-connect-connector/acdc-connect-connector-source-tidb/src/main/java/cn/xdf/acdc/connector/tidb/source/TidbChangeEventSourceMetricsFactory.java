package cn.xdf.acdc.connector.tidb.source;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class TidbChangeEventSourceMetricsFactory implements ChangeEventSourceMetricsFactory {

    private final TidbStreamingChangeEventSourceMetrics streamingMetrics;

    public TidbChangeEventSourceMetricsFactory(final TidbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSourceMetrics getSnapshotMetrics(final CdcSourceTaskContext taskContext,
                                                               final ChangeEventQueueMetrics changeEventQueueMetrics, final EventMetadataProvider eventMetadataProvider) {
        return new TidbSnapshotChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public StreamingChangeEventSourceMetrics getStreamingMetrics(final CdcSourceTaskContext taskContext,
                                                                 final ChangeEventQueueMetrics changeEventQueueMetrics, final EventMetadataProvider eventMetadataProvider) {
        return streamingMetrics;
    }
}
