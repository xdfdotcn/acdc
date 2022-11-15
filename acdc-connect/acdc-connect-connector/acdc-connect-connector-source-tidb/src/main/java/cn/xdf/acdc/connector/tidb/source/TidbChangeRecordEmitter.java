package cn.xdf.acdc.connector.tidb.source;

import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import java.util.List;

public class TidbChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final Operation operation;

    private final OffsetContext offset;

    private final List<TicdcEventColumn> before;

    private final List<TicdcEventColumn> after;

    public TidbChangeRecordEmitter(final OffsetContext offset, final Clock clock, final Operation operation,
                                   final List<TicdcEventColumn> before, final List<TicdcEventColumn> after) {
        super(offset, clock);
        this.offset = offset;
        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    @Override
    public OffsetContext getOffset() {
        return offset;
    }

    @Override
    protected Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        Object[] result = null;
        if (before != null) {
            result = before.stream().map(TicdcEventColumn::getV).toArray();
        }
        return result;
    }

    @Override
    protected Object[] getNewColumnValues() {
        Object[] result = null;
        if (after != null) {
            result = after.stream().map(TicdcEventColumn::getV).toArray();
        }
        return result;
    }
}
