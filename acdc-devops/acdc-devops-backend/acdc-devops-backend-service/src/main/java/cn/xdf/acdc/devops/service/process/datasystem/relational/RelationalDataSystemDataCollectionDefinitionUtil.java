package cn.xdf.acdc.devops.service.process.datasystem.relational;

import cn.xdf.acdc.devops.core.constant.SourceTypeConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinitionExtendPropertyName;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Keyword;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class RelationalDataSystemDataCollectionDefinitionUtil {
    
    public static final int DEFAULT_DECIMAL_LENGTH = 10;
    
    public static final int DEFAULT_DECIMAL_SCALE = 3;
    
    /**
     * Get connect schema by column type.
     *
     * @param columnType column type
     * @return connect schema
     */
    public static Schema getConnectSchema(final String columnType) {
        String[] columnTypeSplit = columnType.split(Symbol.SPACE);
        boolean unsigned = columnTypeSplit.length > 1 && Objects.equals(columnTypeSplit[1], Keyword.UNSIGNED_SYMBAL);
        
        switch (getTypeSignature(columnTypeSplit[0])) {
            case SourceTypeConstant.LONGTEXT:
            case SourceTypeConstant.TEXT:
            case SourceTypeConstant.MEDIUMTEXT:
            case SourceTypeConstant.TINYTEXT:
            case SourceTypeConstant.BLOB:
            case SourceTypeConstant.LONGBLOB:
            case SourceTypeConstant.CHAR:
            case SourceTypeConstant.VARCHAR:
                return SchemaBuilder.string().build();
            case SourceTypeConstant.JSON:
                return Json.builder().build();
            case SourceTypeConstant.BIT:
                int bitLength = Integer.parseInt(Objects.requireNonNull(getContentInParenthesis(columnTypeSplit[0])));
                if (bitLength == 1) {
                    return SchemaBuilder.bool().build();
                }
                return SchemaBuilder.int8().build();
            case SourceTypeConstant.ENUM:
                return SchemaBuilder.int8().build();
            case SourceTypeConstant.TINYINT:
                return unsigned ? SchemaBuilder.int16().build() : SchemaBuilder.int8().build();
            case SourceTypeConstant.SMALLINT:
                return unsigned ? SchemaBuilder.int32().build() : SchemaBuilder.int16().build();
            case SourceTypeConstant.MEDIUMINT:
                return SchemaBuilder.int32().build();
            case SourceTypeConstant.INT:
                return unsigned ? SchemaBuilder.int64().build() : SchemaBuilder.int32().build();
            case SourceTypeConstant.BIGINT:
                if (unsigned) {
                    log.info("Unsupported type: bigint unsigned.");
                }
                return SchemaBuilder.int64().build();
            case SourceTypeConstant.DECIMAL:
                int length = getLength(columnTypeSplit[0]);
                int scale = getScale(columnTypeSplit[0]);
                return SpecialValueDecimal.builder(DecimalMode.PRECISE, length, scale).build();
            case SourceTypeConstant.DOUBLE:
                if (unsigned) {
                    log.info("Unsupported type: double unsigned.");
                }
                return SchemaBuilder.float64().build();
            case SourceTypeConstant.FLOAT:
                if (unsigned) {
                    return SchemaBuilder.float64().build();
                }
                return SchemaBuilder.float32().build();
            case SourceTypeConstant.DATE:
                return org.apache.kafka.connect.data.Date.builder().build();
            case SourceTypeConstant.TIME:
                return org.apache.kafka.connect.data.Time.builder().build();
            case SourceTypeConstant.TIMESTAMP:
            case SourceTypeConstant.DATETIME:
                return org.apache.kafka.connect.data.Timestamp.builder().build();
            default:
                return SchemaBuilder.string().build();
        }
    }
    
    /**
     * Get field extend properties by field type.
     *
     * @param type column type
     * @return field extend properties
     */
    public static Map<DataFieldDefinitionExtendPropertyName, Object> getFieldExtendProperties(final String type) {
        Map<DataFieldDefinitionExtendPropertyName, Object> extendProperties = new HashMap<>();
        if (type.endsWith(Keyword.UNSIGNED_SYMBAL)) {
            extendProperties.put(DataFieldDefinitionExtendPropertyName.UNSIGNED, true);
        }
        if (type.endsWith(Keyword.UNSIGNED_ZEROFILL_SYMBAL)) {
            extendProperties.put(DataFieldDefinitionExtendPropertyName.UNSIGNED_ZEROFILL, true);
        }
        
        String simpleColumnType = type.split(Symbol.SPACE)[0];
        String typeSignature = getTypeSignature(simpleColumnType);
        String contentInParenthesis = getContentInParenthesis(type);
        if (Objects.equals(typeSignature, Keyword.ENUM_TYPE) || Objects.equals(typeSignature, Keyword.SET_TYPE)) {
            extendProperties.put(DataFieldDefinitionExtendPropertyName.CANDIDATES, contentInParenthesis);
        } else if (contentInParenthesis != null) {
            if (contentInParenthesis.contains(Symbol.COMMA)) {
                int length = Integer.parseInt(contentInParenthesis.substring(0, contentInParenthesis.indexOf(Symbol.COMMA)).trim());
                int scale = Integer.parseInt(contentInParenthesis.substring(contentInParenthesis.indexOf(Symbol.COMMA) + 1).trim());
                extendProperties.put(DataFieldDefinitionExtendPropertyName.LENGTH, length);
                extendProperties.put(DataFieldDefinitionExtendPropertyName.SCALE, scale);
            } else {
                extendProperties.put(DataFieldDefinitionExtendPropertyName.LENGTH, Integer.parseInt(contentInParenthesis.trim()));
            }
        }
        
        return extendProperties;
    }
    
    private static String getContentInParenthesis(final String type) {
        if (!type.contains(Symbol.OPEN_PARENTHESIS)) {
            return null;
        }
        return type.substring(type.indexOf(Symbol.OPEN_PARENTHESIS) + 1, type.indexOf(Symbol.CLOSE_PARENTHESIS));
    }
    
    private static String getTypeSignature(final String type) {
        if (!type.contains(Symbol.OPEN_PARENTHESIS)) {
            return type;
        }
        return type.substring(0, type.indexOf(Symbol.OPEN_PARENTHESIS));
    }
    
    private static int getScale(final String type) {
        if (!type.contains("(")) {
            return DEFAULT_DECIMAL_SCALE;
        }
        String scaleString = type.substring(type.indexOf(Symbol.COMMA) + 1, type.indexOf(Symbol.CLOSE_PARENTHESIS));
        return Integer.parseInt(scaleString.trim());
    }
    
    private static int getLength(final String type) {
        if (!type.contains("(")) {
            return DEFAULT_DECIMAL_LENGTH;
        }
        String lengthString = type.substring(type.indexOf(Symbol.OPEN_PARENTHESIS) + 1, type.indexOf(Symbol.COMMA));
        return Integer.parseInt(lengthString.trim());
    }
}
