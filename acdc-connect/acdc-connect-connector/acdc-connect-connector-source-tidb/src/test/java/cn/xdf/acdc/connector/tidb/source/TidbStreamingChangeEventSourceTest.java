package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.TidbConnector;
import cn.xdf.acdc.connector.tidb.reader.BaseReaderWithKafkaEnv;
import cn.xdf.acdc.connector.tidb.util.DelayStrategy;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class TidbStreamingChangeEventSourceTest extends BaseReaderWithKafkaEnv {

    private ChangeEventQueue<DataChangeEvent> queue;

    private volatile boolean isRunning;

    /**
     * Get streaming change event source.
     *
     * @return streaming change event source
     */
    public StreamingChangeEventSource getStreamingChangeEventSource() {
        final Clock clock = Clock.system();
        TidbConnectorConfig connectorConfig = TidbConnectorConfigTest.getTidbConnectorConfig("sourceBootstrap", "sourceTopic", "consumerGroupId", 1);
        final TopicSelector<TableId> topicSelector = TopicSelector.defaultSelector(connectorConfig,
            (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.catalog(), tableId.table()));
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        final TidbValueConverters valueConverterProvider = TidbValueConverters.getValueConverters(connectorConfig);

        TidbDatabaseSchema schema = new TidbDatabaseSchema(connectorConfig, valueConverterProvider, topicSelector, schemaNameAdjuster);

        TidbSourceTaskContext taskContext = new TidbSourceTaskContext(connectorConfig, schema, getReader());
        // Set up the task record queue ...
        queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(TidbConnectorTask.CONTEXT_NAME))
                .build();
        ErrorHandler errorHandler = new ErrorHandler(TidbConnector.class, connectorConfig.getLogicalName(), queue);
        TidbEventMetadataProvider tidbEventMetadataProvider = new TidbEventMetadataProvider();
        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                tidbEventMetadataProvider,
                schemaNameAdjuster);
        TidbStreamingChangeEventSourceMetrics streamingMetrics = new TidbStreamingChangeEventSourceMetrics(taskContext, queue, tidbEventMetadataProvider);
        TidbChangeEventSourceFactory tidbChangeEventSourceFactory = new TidbChangeEventSourceFactory(connectorConfig, errorHandler, dispatcher, clock, taskContext, streamingMetrics, queue);
        return tidbChangeEventSourceFactory.getStreamingChangeEventSource(taskContext.getOffsetContext());
    }

    @Test
    public void testExecuteShouldAddEventToQueueInOrder() {
        isRunning = true;
        StreamingChangeEventSource streamingChangeEventSource = getStreamingChangeEventSource();
        new Thread(() -> {
            try {
                streamingChangeEventSource.execute(() -> isRunning);
            } catch (InterruptedException e) {
                log.error("Current thread is interrupted, {}", e);
            }
        }).start();
        DelayStrategy delayStrategy = DelayStrategy.exponentialWithTimeoutException(10000L);
        List<DataChangeEvent> list = new CopyOnWriteArrayList<>();
        delayStrategy.sleepWhenNotTimeout(() -> {
            try {
                list.addAll(queue.poll());
                if (list.size() == 5) {
                    return false;
                }
            } catch (InterruptedException e) {
                log.error("Current thread is interrupted, {}", e);
            }
            return true;
        });
        list.get(0).getRecord().value();
        Assert.assertEquals(10, ((Struct) ((Struct) list.get(0).getRecord().value()).get("after")).get("col1"));
        Assert.assertEquals(11, ((Struct) ((Struct) list.get(1).getRecord().value()).get("after")).get("col1"));
        Assert.assertEquals(12, ((Struct) ((Struct) list.get(2).getRecord().value()).get("after")).get("col1"));
        Assert.assertEquals(13, ((Struct) ((Struct) list.get(3).getRecord().value()).get("after")).get("col1"));
        Assert.assertEquals(14, ((Struct) ((Struct) list.get(4).getRecord().value()).get("after")).get("col1"));
        isRunning = false;
    }

}
