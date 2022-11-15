package cn.xdf.acdc.connector.tidb.source;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class TidbEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(final DataCollectionId source, final OffsetContext offset, final Object key, final Struct value) {
        return null;
    }

    @Override
    public Map<String, String> getEventSourcePosition(final DataCollectionId source, final OffsetContext offset, final Object key, final Struct value) {
        return Collect.hashMapOf(TidbOffsetContext.SERVER_PARTITION_KEY, (String) offset.getPartition().get(TidbOffsetContext.SERVER_PARTITION_KEY),
                TidbOffsetContext.READER_PARTITION, offset.getOffset().get(TidbOffsetContext.READER_PARTITION).toString(),
                TidbOffsetContext.READER_OFFSET, offset.getOffset().get(TidbOffsetContext.READER_OFFSET).toString()
        );
    }

    @Override
    public String getTransactionId(final DataCollectionId source, final OffsetContext offset, final Object key, final Struct value) {
        return null;
    }
}
