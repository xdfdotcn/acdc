package cn.xdf.acdc.connector.tidb.source;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;

public class TidbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource {

    private final CommonConnectorConfig connectorConfig;

    public TidbSnapshotChangeEventSource(final CommonConnectorConfig connectorConfig, final OffsetContext previousOffset, final SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected SnapshotResult doExecute(final ChangeEventSourceContext context, final SnapshotContext snapshotContext, final SnapshottingTask snapshottingTask) {
        return SnapshotResult.skipped(new TidbOffsetContext(connectorConfig));
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(final OffsetContext previousOffset) {
        return new SnapshottingTask(false, false);
    }

    @Override
    protected SnapshotContext prepare(final ChangeEventSourceContext changeEventSourceContext) {
        return new SnapshotContext();
    }
}
