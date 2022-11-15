package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.reader.TidbDataReader;
import io.debezium.connector.common.CdcSourceTaskContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TidbSourceTaskContext extends CdcSourceTaskContext {

    private final TidbDatabaseSchema schema;

    private final TidbDataReader reader;

    private final Map<Thread, TidbOffsetContext> offsetContextHolder = new ConcurrentHashMap<>();

    private final TidbConnectorConfig connectorConfig;

    public TidbSourceTaskContext(final TidbConnectorConfig config, final TidbDatabaseSchema schema, final TidbDataReader tidbDataReader) {
        super(config.getContextName(), config.getLogicalName() + "-" + config.getTaskId(), schema::tableIds);
        this.schema = schema;
        this.reader = tidbDataReader;
        connectorConfig = config;
    }

    /**
     * Get database schema.
     *
     * @return database schema
     */
    public TidbDatabaseSchema getSchema() {
        return schema;
    }

    /**
     * Get tidb data reader.
     *
     * @return tidb data reader
     */
    public TidbDataReader getTidbDataReader() {
        return reader;
    }

    /**
     * Get tidb offset context.
     *
     * @return tidb offset context
     */
    public TidbOffsetContext getOffsetContext() {
        if (offsetContextHolder.get(Thread.currentThread()) == null) {
            offsetContextHolder.put(Thread.currentThread(), new TidbOffsetContext(connectorConfig));
        }
        return offsetContextHolder.get(Thread.currentThread());
    }

    /**
     * Get offset context holder for metrics.
     *
     * @return offset context holder
     */
    public Map<Thread, TidbOffsetContext> getOffsetContextHolder() {
        return offsetContextHolder;
    }

}
