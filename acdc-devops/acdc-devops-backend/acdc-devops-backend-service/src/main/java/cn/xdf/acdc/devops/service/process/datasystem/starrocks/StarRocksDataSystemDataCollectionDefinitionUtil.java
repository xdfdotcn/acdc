package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemConstant.Metadata.FieldType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Map;

public class StarRocksDataSystemDataCollectionDefinitionUtil {
    
    // io.debezium.data.SpecialValueDecimal.PRECISION_PARAMETER_KEY
    private static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";
    
    /**
     * Get type from connect schema.
     *
     * @param connectSchema connect schema
     * @return type
     */
    public static String getTypeFromSchema(final Schema connectSchema) {
        switch (connectSchema.type()) {
            case STRING:
                return FieldType.VARCHAR_WITH_MAX_LENGTH;
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case INT8:
                return FieldType.TINYINT;
            case INT16:
                return FieldType.SMALLINT;
            case INT32:
                if (Date.LOGICAL_NAME.equals(connectSchema.name())) {
                    return FieldType.DATE;
                }
                if (Time.LOGICAL_NAME.equals(connectSchema.name())) {
                    return FieldType.VARCHAR_WITH_MAX_LENGTH;
                }
                return FieldType.INT;
            case INT64:
                if (Timestamp.LOGICAL_NAME.equals(connectSchema.name())) {
                    return FieldType.DATETIME;
                }
                return FieldType.BIGINT;
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(connectSchema.name())) {
                    String precisionAndScale = getDecimalPrecisionAndScale(connectSchema.parameters());
                    return FieldType.DECIMAL + precisionAndScale;
                }
                throw new UnsupportedOperationException(String.format("Unknown schema type: %s.", connectSchema.type()));
            case FLOAT32:
                return FieldType.FLOAT;
            case FLOAT64:
                return FieldType.DOUBLE;
            default:
                throw new UnsupportedOperationException(String.format("Unknown schema type: %s.", connectSchema.type()));
        }
    }
    
    private static String getDecimalPrecisionAndScale(final Map<String, String> parameters) {
        String precision = parameters.get(PRECISION_PARAMETER_KEY);
        String scale = parameters.get(Decimal.SCALE_FIELD);
        return Symbol.OPEN_PARENTHESIS + precision + Symbol.COMMA + scale + Symbol.CLOSE_PARENTHESIS;
    }
}
