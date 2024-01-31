package cn.xdf.acdc.devops.service.process.widetable.sql;

import io.debezium.data.Json;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Objects;

public class ConnectSchemaToCalciteSqlType {
    
    public static final Schema CONNECT_BOOLEAN = SchemaBuilder.bool().build();
    
    public static final Schema CONNECT_STRING = SchemaBuilder.string().build();
    
    public static final Schema CONNECT_JSON = Json.builder().build();
    
    public static final Schema CONNECT_INT8 = SchemaBuilder.int8().build();
    
    public static final Schema CONNECT_INT16 = SchemaBuilder.int16().build();
    
    public static final Schema CONNECT_INT32 = SchemaBuilder.int32().build();
    
    public static final Schema CONNECT_INT64 = SchemaBuilder.int64().build();
    
    public static final Schema CONNECT_FLOAT32 = SchemaBuilder.float32().build();
    
    public static final Schema CONNECT_FLOAT64 = SchemaBuilder.float64().build();
    
    public static final Schema CONNECT_DATE = org.apache.kafka.connect.data.Date.builder().build();
    
    public static final Schema CONNECT_TIME = org.apache.kafka.connect.data.Time.builder();
    
    public static final Schema CONNECT_TIMESTAMP = org.apache.kafka.connect.data.Timestamp.builder();
    
    /**
     * Get calcite sql type by connect schema.
     *
     * @param connectSchema connect schema
     * @return sql type name
     */
    public static SqlTypeName getCalciteSqlType(final Schema connectSchema) {
        if (connectSchema.equals(CONNECT_BOOLEAN)) {
            return SqlTypeName.BOOLEAN;
        }
        if (connectSchema.equals(CONNECT_STRING) || connectSchema.equals(CONNECT_JSON)) {
            return SqlTypeName.VARCHAR;
        }
        if (connectSchema.equals(CONNECT_INT8)) {
            return SqlTypeName.TINYINT;
        }
        if (connectSchema.equals(CONNECT_INT16)) {
            return SqlTypeName.SMALLINT;
        }
        if (connectSchema.equals(CONNECT_INT32)) {
            return SqlTypeName.INTEGER;
        }
        if (connectSchema.equals(CONNECT_INT64)) {
            return SqlTypeName.BIGINT;
        }
        if (connectSchema.equals(CONNECT_FLOAT32)) {
            return SqlTypeName.FLOAT;
        }
        if (connectSchema.equals(CONNECT_FLOAT64)) {
            return SqlTypeName.DOUBLE;
        }
        if (Objects.equals(connectSchema.name(), CONNECT_DATE.name())) {
            return SqlTypeName.DATE;
        }
        if (Objects.equals(connectSchema.name(), CONNECT_TIME.name())) {
            return SqlTypeName.TIME;
        }
        if (Objects.equals(connectSchema.name(), CONNECT_TIMESTAMP.name())) {
            return SqlTypeName.TIMESTAMP;
        }
        if (Objects.equals(connectSchema.name(), Decimal.LOGICAL_NAME)) {
            return SqlTypeName.DECIMAL;
        }
        
        throw new UnsupportedOperationException("Unsupported connect schema: " + connectSchema);
    }
}
