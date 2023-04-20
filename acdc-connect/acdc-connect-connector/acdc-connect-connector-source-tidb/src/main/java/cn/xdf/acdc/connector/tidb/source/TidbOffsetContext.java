package cn.xdf.acdc.connector.tidb.source;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
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

    private final Struct sourceInfo;

    private Map<String, Object> offset = new HashMap<>();

    public TidbOffsetContext(final CommonConnectorConfig connectorConfig) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        sourceInfo = new TidbSourceInfoStructMaker().struct(null);
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
        return sourceInfo.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo;
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
        TableId tableId = (TableId) collectionId;
        sourceInfo.put(AbstractSourceInfo.TABLE_NAME_KEY, tableId.table());
    }

    @Override
    public TransactionContext getTransactionContext() {
        return null;
    }
}
