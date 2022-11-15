package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.TidbConnector;
import cn.xdf.acdc.connector.tidb.reader.KafkaTidbOpenProtocolReader;
import cn.xdf.acdc.connector.tidb.reader.TidbDataReader;
import cn.xdf.acdc.connector.tidb.util.Version;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TidbConnectorTask extends BaseSourceTask {

    public static final String CONTEXT_NAME = "tidb-connector-task";

    private volatile TidbSourceTaskContext taskContext;

    private volatile ChangeEventQueue<DataChangeEvent> queue;

    private volatile ErrorHandler errorHandler;

    private volatile TidbDatabaseSchema schema;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    protected ChangeEventSourceCoordinator start(final Configuration config) {
        final Clock clock = Clock.system();
        final TidbConnectorConfig connectorConfig = new TidbConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = getTableIdTopicSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        final TidbValueConverters valueConverterProvider = TidbValueConverters.getValueConverters(connectorConfig);

        this.schema = new TidbDatabaseSchema(connectorConfig, valueConverterProvider, topicSelector, schemaNameAdjuster);
        TidbDataReader tidbDataReader = new KafkaTidbOpenProtocolReader(connectorConfig.getConfig());
        taskContext = new TidbSourceTaskContext(connectorConfig, schema, tidbDataReader);

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new TidbErrorHandler(connectorConfig.getLogicalName(), queue);

        final TidbEventMetadataProvider metadataProvider = new TidbEventMetadataProvider();

        final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        final TidbStreamingChangeEventSourceMetrics streamingMetrics = new TidbStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider);
        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                // Since ticdc transfer data to a kafka topic, we get data from this topic, parse and transfer to another topic,
                // so we use kafka group to manage the previous offset and keep this value null.
                null,
                errorHandler,
                TidbConnector.class,
                connectorConfig,
                new TidbChangeEventSourceFactory(connectorConfig, errorHandler, dispatcher, clock, taskContext, streamingMetrics, queue),
                new TidbChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private TopicSelector<TableId> getTableIdTopicSelector(final TidbConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
            (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.catalog(), tableId.table()));
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
            .map(DataChangeEvent::getRecord)
            .collect(Collectors.toList());
        return sourceRecords;
    }

    @Override
    protected void doStop() {
    }

    @Override
    public void commitRecord(final SourceRecord record) {
        Map<String, ?> partitionOffset = record.sourceOffset();
        int partition = (Integer) partitionOffset.get(TidbOffsetContext.READER_PARTITION);
        long offset = (Long) partitionOffset.get(TidbOffsetContext.READER_OFFSET);
        int eventOrderInBatch = (Integer) partitionOffset.get(TidbOffsetContext.READER_EVENT_ORDER_IN_BATCH);
        taskContext.getTidbDataReader().markTicdcEventAsDone(partition, offset, eventOrderInBatch);
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return TidbConnectorConfig.ALL_FIELDS;
    }

}
