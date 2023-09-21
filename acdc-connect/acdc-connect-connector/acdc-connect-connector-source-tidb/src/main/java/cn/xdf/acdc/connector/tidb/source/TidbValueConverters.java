package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.ticdc.protocol.Types;
import cn.xdf.acdc.connector.tidb.util.BigIntUnsignedHandlingMode;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Base64;

public class TidbValueConverters extends JdbcValueConverters {
    
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    
    public static final String TIME_FORMAT = "HH:mm:ss";
    
    public static final String DECIMAL_PLACEHOLDER = "S";
    
    public static final String DOT = ".";
    
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public TidbValueConverters(final JdbcValueConverters.DecimalMode decimalMode, final TemporalPrecisionMode temporalPrecisionMode,
                               final JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode, final CommonConnectorConfig.BinaryHandlingMode binaryMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, bigIntUnsignedMode, binaryMode);
    }
    
    @Override
    public SchemaBuilder schemaBuilder(final Column column) {
        switch (column.nativeType()) {
            case Types.BOOL_OR_TINYINT:
            case Types.BIT:
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                return SchemaBuilder.int16();
            case Types.INT:
            case Types.MEDIUMINT:
            case Types.ENUM:
            case Types.SET:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                return SchemaBuilder.int32();
            case Types.FLOAT:
            case Types.DOUBLE:
                // values are double precision floating point number which supports 15 digits of mantissa.
                return SchemaBuilder.float64();
            case Types.NULL:
                logger.warn("Unexpected tidb type: NULL");
                return null;
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                return SchemaBuilder.int64();
            case Types.DATE_V1:
            case Types.DATE_V2:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return Date.builder();
                }
                return org.apache.kafka.connect.data.Date.builder();
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return MicroTime.builder();
                }
                if (adaptiveTimePrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return Time.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return MicroTime.builder();
                    }
                    return NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
            case Types.DATETIME:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return Timestamp.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return MicroTimestamp.builder();
                    }
                    return NanoTimestamp.builder();
                }
                return org.apache.kafka.connect.data.Timestamp.builder();
            case Types.YEAR:
                return Year.builder();
            // StringEscapeUtils.unescapeJava(s);
    
            case Types.VARCHAR_OR_VARBINARY_V1:
            case Types.VARCHAR_OR_VARBINARY_V2:
            case Types.CHAR_OR_BINARY:
                // TODO Base64
            case Types.TINYTEXT_OR_TINYBLOB:
            case Types.MEDIUMTEXT_OR_MEDIUMBLOB:
            case Types.LONGTEXT_OR_LONGBLOB:
            case Types.TEXT_OR_BLOB:
                return SchemaBuilder.string();
            case Types.JSON:
                return Json.builder();
            case Types.DECIMAL:
                return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().get());
            case Types.GEOMETRY:
            default:
                throw new ConnectException("Unsupported tidb type: " + column.nativeType());
        }
    }
    
    @Override
    public ValueConverter converter(final Column column, final Field fieldDefn) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog client ...
        switch (column.nativeType()) {
            case Types.BOOL_OR_TINYINT:
            case Types.BIT:
                // values are an 8-bit unsigned integer value between 0 and 255
                return data -> convertTinyInt(column, fieldDefn, data);
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                return data -> convertSmallInt(column, fieldDefn, data);
            case Types.INT:
            case Types.MEDIUMINT:
                // todo check
            case Types.ENUM:
            case Types.SET:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                return data -> convertInteger(column, fieldDefn, data);
            case Types.FLOAT:
                return data -> convertFloat(column, fieldDefn, data);
            case Types.DOUBLE:
                // values are double precision floating point number which supports 15 digits of mantissa.
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NULL:
                return data -> null;
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                return data -> convertBigInt(column, fieldDefn, data);
            case Types.DATE_V1:
            case Types.DATE_V2:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return data -> convertDateToEpochDays(column, fieldDefn, getLocalDate(data));
                }
                return data -> convertDateToEpochDaysAsDate(column, fieldDefn, getLocalDate(data));
            case Types.TIME:
                return data -> convertTime(column, fieldDefn, getLocalTime(data));
            case Types.TIMESTAMP:
            case Types.DATETIME:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return data -> convertTimestampToEpochMillis(column, fieldDefn, getLocalDateTime(data));
                    }
                    if (getTimePrecision(column) <= 6) {
                        return data -> convertTimestampToEpochMicros(column, fieldDefn, getLocalDateTime(data));
                    }
                    return data -> convertTimestampToEpochNanos(column, fieldDefn, getLocalDateTime(data));
                }
                return data -> convertTimestampToEpochMillisAsDate(column, fieldDefn, getLocalDateTime(data));
            case Types.YEAR:
                return data -> convertYearToInt(column, fieldDefn, data);
            // 去除转义
            case Types.VARCHAR_OR_VARBINARY_V1:
            case Types.VARCHAR_OR_VARBINARY_V2:
            case Types.CHAR_OR_BINARY:
                return data -> convertString(column, fieldDefn, StringEscapeUtils.unescapeJava((String) data));
            // Base64 decode
            case Types.TINYTEXT_OR_TINYBLOB:
            case Types.MEDIUMTEXT_OR_MEDIUMBLOB:
            case Types.LONGTEXT_OR_LONGBLOB:
            case Types.TEXT_OR_BLOB:
                return data -> convertString(column, fieldDefn, data == null ? null : new String(Base64.getDecoder().decode((String) data)));
            case Types.JSON:
                return data -> convertJson(column, fieldDefn, data);
            case Types.DECIMAL:
                return data -> convertDecimalWithScaleAdjustedIfNeeded(column, fieldDefn, data);
            case Types.GEOMETRY:
            default:
                throw new ConnectException("Unsupport tidb type: " + column.nativeType());
        }
    }
    
    private Object getLocalDateTime(final Object data) {
        return data == null ? null : LocalDateTime.parse(data.toString(), DateTimeFormatter.ofPattern(DATE_TIME_FORMAT + getDecimalPlaceHolder(data)));
    }
    
    private Object getLocalDate(final Object data) {
        return data == null ? null : LocalDate.parse(data.toString(), DATE_FORMATTER);
    }
    
    private Object getLocalTime(final Object data) {
        return data == null ? null : LocalTime.parse(data.toString(), DateTimeFormatter.ofPattern(TIME_FORMAT + getDecimalPlaceHolder(data)));
    }
    
    private StringBuilder getDecimalPlaceHolder(final Object data) {
        String dateTimeString = data.toString();
        int decimalPointIndex = dateTimeString.indexOf(DOT);
        StringBuilder decimalPlaceHolder = new StringBuilder();
        if (decimalPointIndex != -1 && !dateTimeString.endsWith(DOT)) {
            decimalPlaceHolder.append(DOT);
            for (int i = 0; i < dateTimeString.substring(decimalPointIndex + 1).length(); i++) {
                decimalPlaceHolder.append(DECIMAL_PLACEHOLDER);
            }
        }
        return decimalPlaceHolder;
    }
    
    /**
     * Convert the {@link String} value to a string value used in a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertJson(final Column column, final Field fieldDefn, final Object data) {
        return convertValue(column, fieldDefn, data, "{}", r -> {
            if (data instanceof String) {
                // The SnapshotReader sees JSON values as UTF-8 encoded strings.
                r.deliver(data);
            }
        });
    }
    
    /**
     * Converts a value object for a MySQL {@code YEAR}, which appear in the binlog as an integer though returns from
     * the MySQL JDBC driver as either a short or a {@link java.sql.Date}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a year literal integer value; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @SuppressWarnings("deprecation")
    protected Object convertYearToInt(final Column column, final Field fieldDefn, final Object data) {
        return convertValue(column, fieldDefn, data, 0, r -> {
            if (data instanceof Number) {
                // MySQL JDBC driver sometimes returns a short ...
                r.deliver(java.time.Year.of(((Number) data).intValue()).get(ChronoField.YEAR));
            }
        });
    }
    
    /**
     * Convert decimal with adjusted scale if needed.
     * Same decimal column in one update event may have unique scale, so we need to adjusted it.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a corresponding type determined by decimal mode configuration; never null
     * @return
     */
    protected Object convertDecimalWithScaleAdjustedIfNeeded(final Column column, final Field fieldDefn, final Object data) {
        Object decimal = toBigDecimal(column, fieldDefn, data);
        if (decimal instanceof BigDecimal) {
            decimal = withScaleAdjustedIfNotEqual(column, (BigDecimal) decimal);
            return SpecialValueDecimal.fromLogical(new SpecialValueDecimal((BigDecimal) decimal), decimalMode, column.name());
        }
        return decimal;
    }
    
    private BigDecimal withScaleAdjustedIfNotEqual(Column column, BigDecimal data) {
        if (column.scale().isPresent() && column.scale().get() != data.scale()) {
            data = data.setScale(column.scale().get());
        }
        
        return data;
    }
    
    /**
     * Get tidb value converter.
     *
     * @param configuration tidb connector config
     * @return tidb value converter
     */
    public static TidbValueConverters getValueConverters(final TidbConnectorConfig configuration) {
        
        // Use MySQL-specific converters and schemas for values ...
        TemporalPrecisionMode timePrecisionMode = configuration.getTemporalPrecisionMode();
        
        JdbcValueConverters.DecimalMode decimalMode = configuration.getDecimalMode();
        
        String bigIntUnsignedHandlingModeStr = configuration.getConfig().getString(TidbConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode = BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();
        
        return new TidbValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                configuration.binaryHandlingMode());
    }
    
}
