package cn.xdf.acdc.connector.tidb.source;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TidbOffsetContext implements OffsetContext {

    public static final String SERVER_PARTITION_KEY = "server";

    public static final String READER_PARTITION = "reader-partition";

    public static final String READER_OFFSET = "reader-offset";

    public static final String READER_EVENT_ORDER_IN_BATCH = "reader-event-order-in-batch";

    public static final String READER_EVENT_TYPE = "reader-event-type";

    private final Map<String, String> partition;

    private final SourceInfoStructMaker<AbstractSourceInfo> sourceInfoStructMaker;

    private Map<String, Object> offset = new HashMap<>();
    
    private TableId tableId;

    public TidbOffsetContext(final CommonConnectorConfig connectorConfig) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        sourceInfoStructMaker = connectorConfig.getSourceInfoStructMaker();
    }

    /**
     * Set current offset.
     *
     * @param current current offset
     */
    public void setOffset(final Map<String, Object> current) {
        offset = current;
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoStructMaker.schema();
    }

    @Override
    public Struct getSourceInfo() {
        // Here we should build a new object for data send to task thread can not modify by event() in this one.
        return sourceInfoStructMaker.struct(null).put(AbstractSourceInfo.TABLE_NAME_KEY, tableId.table());
    }

    @Override
    public boolean isSnapshotRunning() {
        return false;
    }

    @Override
    public void markLastSnapshotRecord() {

    }

    @Override
    public void preSnapshotStart() {

    }

    @Override
    public void preSnapshotCompletion() {

    }

    @Override
    public void postSnapshotCompletion() {

    }

    @Override
    public void event(final DataCollectionId collectionId, final Instant timestamp) {
        tableId = (TableId) collectionId;
    }

    @Override
    public TransactionContext getTransactionContext() {
        return null;
    }
}
