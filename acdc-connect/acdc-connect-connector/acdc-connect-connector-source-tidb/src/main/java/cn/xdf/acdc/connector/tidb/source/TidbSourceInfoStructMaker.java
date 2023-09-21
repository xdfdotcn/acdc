package cn.xdf.acdc.connector.tidb.source;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class TidbSourceInfoStructMaker implements SourceInfoStructMaker<AbstractSourceInfo> {

    private static final String TIDB_SOURCE_INFO_SCHEMA_NAME = "cn.xdf.acdc.connector.tidb.Source";

    private final Schema schema;

    public TidbSourceInfoStructMaker() {
        schema = SchemaBuilder.struct()
                .name(TIDB_SOURCE_INFO_SCHEMA_NAME)
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
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
