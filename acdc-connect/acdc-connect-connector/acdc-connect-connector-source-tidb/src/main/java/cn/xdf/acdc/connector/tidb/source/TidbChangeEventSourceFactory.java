package cn.xdf.acdc.connector.tidb.source;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class TidbChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final TidbConnectorConfig configuration;

    private final ErrorHandler errorHandler;

    private final EventDispatcher<TableId> dispatcher;

    private final Clock clock;

    private final TidbSourceTaskContext taskContext;

    private final TidbStreamingChangeEventSourceMetrics streamingMetrics;

    private final ChangeEventQueue<DataChangeEvent> queue;

    public TidbChangeEventSourceFactory(final TidbConnectorConfig configuration, final ErrorHandler errorHandler, final EventDispatcher<TableId> dispatcher, final Clock clock,
                                        final TidbSourceTaskContext taskContext, final TidbStreamingChangeEventSourceMetrics streamingMetrics, final ChangeEventQueue<DataChangeEvent> queue) {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.queue = queue;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(final OffsetContext offsetContext, final SnapshotProgressListener snapshotProgressListener) {
        return new TidbSnapshotChangeEventSource(configuration, offsetContext, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(final OffsetContext offsetContext) {
        return new TidbStreamingChangeEventSource(
                dispatcher,
                errorHandler,
                clock,
                taskContext.getSchema(),
                taskContext,
                taskContext.getTidbDataReader(),
                streamingMetrics);
    }
}
