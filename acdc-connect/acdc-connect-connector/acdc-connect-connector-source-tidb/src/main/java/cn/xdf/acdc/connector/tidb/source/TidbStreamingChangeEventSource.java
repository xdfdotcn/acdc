package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.reader.Event;
import cn.xdf.acdc.connector.tidb.reader.EventType;
import cn.xdf.acdc.connector.tidb.reader.TidbDataReader;
import com.pingcap.ticdc.cdc.TicdcEventData;
import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import com.pingcap.ticdc.cdc.value.TicdcEventDDL;
import com.pingcap.ticdc.cdc.value.TicdcEventResolve;
import com.pingcap.ticdc.cdc.value.TicdcEventRowChange;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class TidbStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final String TICDC_EVENT_DELETE = "d";

    private static final String TICDC_EVENT_UPSERT = "u";

    private static final String DATA_EVENT_TYPE_DELETE = "delete";

    private static final String DATA_EVENT_TYPE_INSERT = "insert";

    private static final String DATA_EVENT_TYPE_UPDATE = "update";

    private static final long RUNNING_SLEEP_INTERVAL_MILLISECONDS = 200;

    private final TidbSourceTaskContext taskContext;

    private final EventDispatcher<TableId> eventDispatcher;

    private final ErrorHandler errorHandler;

    private final Clock clock;

    private final TidbStreamingChangeEventSourceMetrics metrics;

    private final EnumMap<EventType, BlockingConsumer<Event>> eventHandlers = new EnumMap<>(EventType.class);

    private final TidbDatabaseSchema schema;

    private final TidbDataReader reader;

    public TidbStreamingChangeEventSource(final EventDispatcher<TableId> dispatcher, final ErrorHandler errorHandler, final Clock clock, final TidbDatabaseSchema schema,
                                          final TidbSourceTaskContext taskContext, final TidbDataReader reader, final TidbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.taskContext = taskContext;
        this.clock = clock;
        this.eventDispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.metrics = streamingMetrics;
        this.schema = schema;
        this.reader = reader;
    }

    @Override
    public void execute(final ChangeEventSourceContext context) throws InterruptedException {
        eventHandlers.put(EventType.ROW_CHANGED_EVENT, this::handleRowChangedEvent);
        eventHandlers.put(EventType.DDL_EVENT, this::handleDdlEvent);
        eventHandlers.put(EventType.RESOLVED_EVENT, this::handleResolvedEvent);
        eventHandlers.put(EventType.RUNNER_STOP_EVENT, this::handleRunnerStopEvent);
        try {
            reader.registerEventListener(this::handleEvent);
            reader.doReading();
            while (context.isRunning()) {
                Thread.sleep(RUNNING_SLEEP_INTERVAL_MILLISECONDS);
            }
        } finally {
            reader.close();
        }
    }

    private void handleEvent(final Event event) {
        if (event == null) {
            return;
        }
        try {
            // Forward the event to the handler ...
            eventHandlers.getOrDefault(event.getType(), this::ignoreEvent).accept(event);
        } catch (InterruptedException e) {
            // Most likely because the reader was stopped and our thread was interrupted ...
            Thread.currentThread().interrupt();
            eventHandlers.clear();
            log.info("Stopped fetching tidb data from kafka due to thread interruption");
        } catch (ConnectException e) {
            errorHandler.setProducerThrowable(e);
            log.error("handle event error: {}", e.getMessage());
        }
    }

    protected void ignoreEvent(final Event event) {
        log.trace("Ignoring event due to missing handler: {}.", event);
    }

    private void handleRunnerStopEvent(final Event event) {
        String msg = "All reader runners are down.";
        errorHandler.setProducerThrowable(event.getData() == null ? new RetriableException(msg) : (Throwable) event.getData());
    }

    private void handleResolvedEvent(final Event event) {
        checkEventType(event.getData() instanceof TicdcEventData && ((TicdcEventData) event.getData()).getTicdcEventValue() instanceof TicdcEventResolve,
                "Event data must be a instance of TicdcEventResolve");
        TicdcEventResolve ticdcEventResolve = (TicdcEventResolve) ((TicdcEventData) event.getData()).getTicdcEventValue();
        taskContext.getOffsetContext().setOffset(offset(ticdcEventResolve.getKafkaPartition(), ticdcEventResolve.getKafkaOffset(), event.getOrder(), event.getType().getDesc()));
        markTicdcEventAsDone(ticdcEventResolve.getKafkaPartition(), ticdcEventResolve.getKafkaOffset(), event.getOrder());
    }

    private void handleDdlEvent(final Event event) {
        checkEventType(event.getData() instanceof TicdcEventData && ((TicdcEventData) event.getData()).getTicdcEventValue() instanceof TicdcEventDDL,
                "Event data must be a instance of TicdcEventDDL");
        TicdcEventDDL ticdcEventDDL = (TicdcEventDDL) ((TicdcEventData) event.getData()).getTicdcEventValue();
        taskContext.getOffsetContext().setOffset(offset(ticdcEventDDL.getKafkaPartition(), ticdcEventDDL.getKafkaOffset(), event.getOrder(), event.getType().getDesc()));
        markTicdcEventAsDone(ticdcEventDDL.getKafkaPartition(), ticdcEventDDL.getKafkaOffset(), event.getOrder());
    }

    private void handleRowChangedEvent(final Event event) throws InterruptedException {
        checkEventType(event.getData() instanceof TicdcEventData && ((TicdcEventData) event.getData()).getTicdcEventValue() instanceof TicdcEventRowChange,
                "Event data must be a instance of TicdcEventRowChange");
        String databaseName = ((TicdcEventData) event.getData()).getTicdcEventKey().getScm();
        String tableName = ((TicdcEventData) event.getData()).getTicdcEventKey().getTbl();
        TicdcEventRowChange ticdcEventRowChange = (TicdcEventRowChange) ((TicdcEventData) event.getData()).getTicdcEventValue();
        TidbOffsetContext offsetContext = taskContext.getOffsetContext();
        offsetContext.setOffset(offset(ticdcEventRowChange.getKafkaPartition(), ticdcEventRowChange.getKafkaOffset(), event.getOrder(), event.getType().getDesc()));
        if (!schema.isIncludedTable(schema.getTableId(databaseName, null, tableName))) {
            markTicdcEventAsDone(ticdcEventRowChange.getKafkaPartition(), ticdcEventRowChange.getKafkaOffset(), event.getOrder());
            return;
        }
        switch (ticdcEventRowChange.getUpdateOrDelete()) {
            case TICDC_EVENT_DELETE:
                handleChange(offsetContext, DATA_EVENT_TYPE_DELETE,
                    () -> schema.getTableId(databaseName, null, tableName),
                    ticdcEventRowChange::getOldColumns,
                    ticdcEventRowChange::getColumns,
                    (tableId, oldColumns, newColumns) ->
                        eventDispatcher.dispatchDataChangeEvent(tableId, new TidbChangeRecordEmitter(offsetContext, clock, Envelope.Operation.DELETE, newColumns, null)));
                break;
            case TICDC_EVENT_UPSERT:
                if (ticdcEventRowChange.getOldColumns() == null) {
                    // insert
                    handleChange(offsetContext, DATA_EVENT_TYPE_INSERT,
                        () -> schema.getTableId(databaseName, null, tableName),
                        ticdcEventRowChange::getOldColumns,
                        ticdcEventRowChange::getColumns,
                        (tableId, oldColumns, newColumns) ->
                            eventDispatcher.dispatchDataChangeEvent(tableId, new TidbChangeRecordEmitter(offsetContext, clock, Envelope.Operation.CREATE, null, newColumns)));
                } else {
                    // update
                    handleChange(offsetContext, DATA_EVENT_TYPE_UPDATE,
                        () -> schema.getTableId(databaseName, null, tableName),
                        ticdcEventRowChange::getOldColumns,
                        ticdcEventRowChange::getColumns,
                        (tableId, oldColumns, newColumns) ->
                            eventDispatcher.dispatchDataChangeEvent(tableId, new TidbChangeRecordEmitter(offsetContext, clock, Envelope.Operation.UPDATE, oldColumns, newColumns)));
                }
                break;
            default:
                throw new ConnectException("Undefined ticdc event type: " + ticdcEventRowChange.getUpdateOrDelete());
        }
    }

    private void handleChange(final TidbOffsetContext offsetContext, final String changeType, final TableIdProvider tableIdProvider, final ColumnDataProvider oldColumnDataProvider,
                              final ColumnDataProvider newColumnDataProvider, final TicdcChangeEmitter changeEmitter)
            throws InterruptedException {
        // Update table schema if changed.
        schema.updateTableSchemaIfChanged(tableIdProvider.getTableId(), newColumnDataProvider.getData());
        offsetContext.event(tableIdProvider.getTableId(), null);
        changeEmitter.emit(tableIdProvider.getTableId(), oldColumnDataProvider.getData(), newColumnDataProvider.getData());
        if (log.isDebugEnabled()) {
            log.debug("Emitted record, change type: {}, partition:{}, offset:{}",
                    changeType, taskContext.getOffsetContext().getPartition(), taskContext.getOffsetContext().getOffset());
        }
    }

    private void markTicdcEventAsDone(final Integer partition, final Long offset, final int order) {
        reader.markTicdcEventAsDone(partition, offset, order);
    }

    private Map<String, Object> offset(final int kafkaPartition, final long kafkaOffset, final int orderInBatch, final String eventType) {
        return new HashMap<String, Object>() {{
                put(TidbOffsetContext.READER_PARTITION, kafkaPartition);
                put(TidbOffsetContext.READER_OFFSET, kafkaOffset);
                put(TidbOffsetContext.READER_EVENT_ORDER_IN_BATCH, orderInBatch);
                put(TidbOffsetContext.READER_EVENT_TYPE, eventType);
            }};
    }

    private void checkEventType(final boolean checkResult, final String errorMsg) {
        if (checkResult) {
            return;
        }
        throw new ConnectException(errorMsg);
    }

    @FunctionalInterface
    private interface TicdcChangeEmitter {
        void emit(TableId tableId, List<TicdcEventColumn> before, List<TicdcEventColumn> after) throws InterruptedException;
    }

    @FunctionalInterface
    private interface TableIdProvider {
        TableId getTableId();
    }

    @FunctionalInterface
    private interface ColumnDataProvider {
        List<TicdcEventColumn> getData();
    }
}
