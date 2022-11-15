package cn.xdf.acdc.connector.tidb.source;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class TidbSourceInfoStructMaker implements SourceInfoStructMaker {

    private final Schema schema;

    public TidbSourceInfoStructMaker() {
        schema = SchemaBuilder.struct()
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(final AbstractSourceInfo sourceInfo) {
        return new Struct(schema());
    }

}
