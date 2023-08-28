package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.ticdc.protocol.Types;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.relational.Column;
import io.debezium.time.Year;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TidbValueConvertersTest {

    private static final Column BOOL_OR_TINYINT_COLUMN = Column.editor().name("BOOL_OR_TINYINT_COLUMN").nativeType(Types.BOOL_OR_TINYINT).create();

    private static final Column SMALLINT_COLUMN = Column.editor().name("SMALLINT_COLUMN").nativeType(Types.SMALLINT).create();

    private static final Column INT_COLUMN = Column.editor().name("INT_COLUMN").nativeType(Types.INT).create();

    private static final Column FLOAT_COLUMN = Column.editor().name("FLOAT_COLUMN").nativeType(Types.FLOAT).create();

    private static final Column DOUBLE_COLUMN = Column.editor().name("DOUBLE_COLUMN").nativeType(Types.DOUBLE).create();

    private static final Column NULL_COLUMN = Column.editor().name("NULL_COLUMN").nativeType(Types.NULL).create();

    private static final Column TIMESTAMP_COLUMN = Column.editor().name("TIMESTAMP_COLUMN").nativeType(Types.TIMESTAMP).create();

    private static final Column BIGINT_COLUMN = Column.editor().name("BIGINT_COLUMN").nativeType(Types.BIGINT).create();

    private static final Column MEDIUMINT_COLUMN = Column.editor().name("MEDIUMINT_COLUMN").nativeType(Types.MEDIUMINT).create();

    private static final Column DATE_V1_COLUMN = Column.editor().name("DATE_V1_COLUMN").nativeType(Types.DATE_V1).create();

    private static final Column DATE_V2_COLUMN = Column.editor().name("DATE_V2_COLUMN").nativeType(Types.DATE_V2).create();

    private static final Column TIME_COLUMN = Column.editor().name("TIME_COLUMN").nativeType(Types.TIME).create();

    private static final Column DATETIME_COLUMN = Column.editor().name("DATETIME_COLUMN").nativeType(Types.DATETIME).create();

    private static final Column YEAR_COLUMN = Column.editor().name("YEAR_COLUMN").nativeType(Types.YEAR).create();

    private static final Column VARCHAR_OR_VARBINARY_V1_COLUMN = Column.editor().name("VARCHAR_OR_VARBINARY_V1_COLUMN").nativeType(Types.VARCHAR_OR_VARBINARY_V1).create();

    private static final Column BIT_COLUMN = Column.editor().name("BIT_COLUMN").nativeType(Types.BIT).create();

    private static final Column VARCHAR_OR_VARBINARY_V2_COLUMN = Column.editor().name("VARCHAR_OR_VARBINARY_V2_COLUMN").nativeType(Types.VARCHAR_OR_VARBINARY_V2).create();

    private static final Column JSON_COLUMN = Column.editor().name("JSON_COLUMN").nativeType(Types.JSON).create();

    private static final Column DECIMAL_COLUMN = Column.editor().name("DECIMAL_COLUMN").nativeType(Types.DECIMAL).length(TidbDatabaseSchema.MAX_DECIMAL_LENGTH).scale(3).create();
    
    private static final Column DECIMAL_COLUMN_ZERO_SCALE = Column.editor().name("DECIMAL_COLUMN").nativeType(Types.DECIMAL).length(TidbDatabaseSchema.MAX_DECIMAL_LENGTH).scale(0).create();

    private static final Column ENUM_COLUMN = Column.editor().name("ENUM_COLUMN").nativeType(Types.ENUM).create();

    private static final Column SET_COLUMN = Column.editor().name("SET_COLUMN").nativeType(Types.SET).create();

    private static final Column TINYTEXT_OR_TINYBLOB_COLUMN = Column.editor().name("TINYTEXT_OR_TINYBLOB_COLUMN").nativeType(Types.TINYTEXT_OR_TINYBLOB).create();

    private static final Column MEDIUMTEXT_OR_MEDIUMBLOB_COLUMN = Column.editor().name("MEDIUMTEXT_OR_MEDIUMBLOB_COLUMN").nativeType(Types.MEDIUMTEXT_OR_MEDIUMBLOB).create();

    private static final Column LONGTEXT_OR_LONGBLOB_COLUMN = Column.editor().name("LONGTEXT_OR_LONGBLOB_COLUMN").nativeType(Types.LONGTEXT_OR_LONGBLOB).create();

    private static final Column TEXT_OR_BLOB_COLUMN = Column.editor().name("TEXT_OR_BLOB_COLUMN").nativeType(Types.TEXT_OR_BLOB).create();

    private static final Column CHAR_OR_BINARY_COLUMN = Column.editor().name("CHAR_OR_BINARY_COLUMN").nativeType(Types.CHAR_OR_BINARY).create();

    private static final Column GEOMETRY_COLUMN = Column.editor().name("GEOMETRY_COLUMN").nativeType(Types.GEOMETRY).create();

    private TidbConnectorConfig config = TidbConnectorConfigTest.getTidbConnectorConfig("", "", "", 1);

    private TidbValueConverters tidbValueConverters = TidbValueConverters.getValueConverters(config);
    
    @Test
    public void testBigDecimalShouldValueScaleMatchesSchemaScale() {
        Schema decimalSchema = SpecialValueDecimal.builder(config.getDecimalMode(), DECIMAL_COLUMN.length(), DECIMAL_COLUMN.scale().get()).build();
        Assert.assertEquals("0.000",
                tidbValueConverters.converter(DECIMAL_COLUMN, new Field("DECIMAL_FIELD", 0, decimalSchema)).convert("0").toString());
    }
    
    @Test
    public void testBigDecimalShouldValueScaleMatchesSchemaScaleWithMoreScale() {
        Schema decimalSchema = SpecialValueDecimal.builder(config.getDecimalMode(), DECIMAL_COLUMN_ZERO_SCALE.length(), DECIMAL_COLUMN_ZERO_SCALE.scale().get()).build();
        Assert.assertEquals("0",
                tidbValueConverters.converter(DECIMAL_COLUMN_ZERO_SCALE, new Field("DECIMAL_FIELD", 0, decimalSchema)).convert("0.000").toString());
    }
    
    @Test
    public void testSchemaBuilderShouldGetSchemaAsExpect() {
        Assert.assertEquals(SchemaBuilder.int16().build(), tidbValueConverters.schemaBuilder(BOOL_OR_TINYINT_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int16().build(), tidbValueConverters.schemaBuilder(SMALLINT_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int32().build(), tidbValueConverters.schemaBuilder(INT_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.float64().build(), tidbValueConverters.schemaBuilder(FLOAT_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.float64().build(), tidbValueConverters.schemaBuilder(DOUBLE_COLUMN).build());
        Assert.assertNull(tidbValueConverters.schemaBuilder(NULL_COLUMN));
        Assert.assertEquals(Timestamp.builder().build(), tidbValueConverters.schemaBuilder(TIMESTAMP_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int64().build(), tidbValueConverters.schemaBuilder(BIGINT_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int32().build(), tidbValueConverters.schemaBuilder(MEDIUMINT_COLUMN).build());
        Assert.assertEquals(org.apache.kafka.connect.data.Date.builder().build(), tidbValueConverters.schemaBuilder(DATE_V1_COLUMN).build());
        Assert.assertEquals(org.apache.kafka.connect.data.Date.builder().build(), tidbValueConverters.schemaBuilder(DATE_V2_COLUMN).build());
        Assert.assertEquals(org.apache.kafka.connect.data.Time.builder().build(), tidbValueConverters.schemaBuilder(TIME_COLUMN).build());
        Assert.assertEquals(Timestamp.builder().build(), tidbValueConverters.schemaBuilder(DATETIME_COLUMN).build());
        Assert.assertEquals(Year.builder().build(), tidbValueConverters.schemaBuilder(YEAR_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(VARCHAR_OR_VARBINARY_V1_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int16().build(), tidbValueConverters.schemaBuilder(BIT_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(VARCHAR_OR_VARBINARY_V2_COLUMN).build());
        Assert.assertEquals(Json.builder().build(), tidbValueConverters.schemaBuilder(JSON_COLUMN).build());
        Assert.assertEquals(SpecialValueDecimal.builder(config.getDecimalMode(), DECIMAL_COLUMN.length(), DECIMAL_COLUMN.scale().get()).build(),
                tidbValueConverters.schemaBuilder(DECIMAL_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int32().build(), tidbValueConverters.schemaBuilder(ENUM_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.int32().build(), tidbValueConverters.schemaBuilder(SET_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(TINYTEXT_OR_TINYBLOB_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(MEDIUMTEXT_OR_MEDIUMBLOB_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(LONGTEXT_OR_LONGBLOB_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(TEXT_OR_BLOB_COLUMN).build());
        Assert.assertEquals(SchemaBuilder.string().build(), tidbValueConverters.schemaBuilder(CHAR_OR_BINARY_COLUMN).build());
        try {
            tidbValueConverters.schemaBuilder(GEOMETRY_COLUMN);
        } catch (ConnectException e) {
            Assert.assertTrue(e.getMessage().contains("Unsupported tidb type"));
        }
    }

    @Test
    public void testConverterShouldGetValueAsExpectWithConverter() {
        Assert.assertEquals(Short.valueOf("1"), tidbValueConverters.converter(BOOL_OR_TINYINT_COLUMN, new Field("BOOL_OR_TINYINT_FIELD", 0, SchemaBuilder.int8().build())).convert(1));
        Assert.assertEquals(Short.valueOf("1"), tidbValueConverters.converter(SMALLINT_COLUMN, new Field("SMALLINT_FIELD", 0, SchemaBuilder.int16().build())).convert(1));
        Assert.assertEquals(1, tidbValueConverters.converter(INT_COLUMN, new Field("INT_FIELD", 0, SchemaBuilder.int32().build())).convert(1));
        Assert.assertEquals(1.1d, tidbValueConverters.converter(FLOAT_COLUMN, new Field("FLOAT_FIELD", 0, SchemaBuilder.float64().build())).convert(1.1));
        Assert.assertEquals(1.23d, tidbValueConverters.converter(DOUBLE_COLUMN, new Field("DOUBLE_FIELD", 0, SchemaBuilder.float64().build())).convert(1.23));
        Assert.assertEquals(Date.from(LocalDateTime.parse("1973-12-30 15:30:00", DateTimeFormatter.ofPattern(TidbValueConverters.DATE_TIME_FORMAT)).atZone(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(TIMESTAMP_COLUMN, new Field("TIMESTAMP_FIELD", 0, Timestamp.builder().build())).convert("1973-12-30 15:30:00"));
        Assert.assertEquals(Date.from(LocalDateTime.parse("1973-12-30 15:30:00.321", DateTimeFormatter.ofPattern(TidbValueConverters.DATE_TIME_FORMAT + ".SSS")).atZone(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(TIMESTAMP_COLUMN, new Field("TIMESTAMP_FIELD", 0, Timestamp.builder().build())).convert("1973-12-30 15:30:00.321"));
        Assert.assertNotEquals(
                Date.from(LocalDateTime.parse("1973-12-30 15:30:00.321", DateTimeFormatter.ofPattern(TidbValueConverters.DATE_TIME_FORMAT + ".SSS")).atZone(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(TIMESTAMP_COLUMN, new Field("TIMESTAMP_FIELD", 0, Timestamp.builder().build())).convert("1973-12-30 15:30:00.322"));
        Assert.assertEquals(100L, tidbValueConverters.converter(BIGINT_COLUMN, new Field("BIGINT_FIELD", 0, SchemaBuilder.int64().build())).convert(100));
        Assert.assertEquals(100, tidbValueConverters.converter(MEDIUMINT_COLUMN, new Field("MEDIUMINT_FIELD", 0, SchemaBuilder.int32().build())).convert(100));
        Assert.assertEquals(Date.from(LocalDate.parse("2000-01-01", TidbValueConverters.DATE_FORMATTER).atStartOfDay(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(DATE_V1_COLUMN, new Field("DATE_V1_FIELD", 0, org.apache.kafka.connect.data.Date.builder().build())).convert("2000-01-01"));
        Assert.assertEquals(Date.from(LocalDate.parse("2001-01-01", TidbValueConverters.DATE_FORMATTER).atStartOfDay(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(DATE_V2_COLUMN, new Field("DATE_V2_FIELD", 0, org.apache.kafka.connect.data.Date.builder().build())).convert("2001-01-01"));

        Assert.assertEquals(getDateFromTime("12:12:12", ""),
                tidbValueConverters.converter(TIME_COLUMN, new Field("TIME_FIELD", 0, org.apache.kafka.connect.data.Time.builder().build())).convert("12:12:12"));
        Assert.assertEquals(getDateFromTime("12:12:12.123", ".SSS"),
                tidbValueConverters.converter(TIME_COLUMN, new Field("TIME_FIELD", 0, org.apache.kafka.connect.data.Time.builder().build())).convert("12:12:12.123"));
        Assert.assertNotEquals(getDateFromTime("12:12:12.123", ".SSS"),
                tidbValueConverters.converter(TIME_COLUMN, new Field("TIME_FIELD", 0, org.apache.kafka.connect.data.Time.builder().build())).convert("12:12:12.122"));
        Assert.assertEquals(Date.from(LocalDateTime.parse("1973-12-30 15:30:00", DateTimeFormatter.ofPattern(TidbValueConverters.DATE_TIME_FORMAT)).atZone(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(DATETIME_COLUMN, new Field("DATETIME_FIELD", 0, Timestamp.builder().build())).convert("1973-12-30 15:30:00"));
        Assert.assertEquals(Date.from(LocalDateTime.parse("1973-12-30 15:30:00.123", DateTimeFormatter.ofPattern(TidbValueConverters.DATE_TIME_FORMAT + ".SSS")).atZone(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(DATETIME_COLUMN, new Field("DATETIME_FIELD", 0, Timestamp.builder().build())).convert("1973-12-30 15:30:00.123"));
        Assert.assertNotEquals(
                Date.from(LocalDateTime.parse("1973-12-30 15:30:00.123", DateTimeFormatter.ofPattern(TidbValueConverters.DATE_TIME_FORMAT + ".SSS")).atZone(ZoneId.of("UTC")).toInstant()),
                tidbValueConverters.converter(DATETIME_COLUMN, new Field("DATETIME_FIELD", 0, Timestamp.builder().build())).convert("1973-12-30 15:30:00.122"));
        Assert.assertEquals(2021, tidbValueConverters.converter(YEAR_COLUMN, new Field("YEAR_FIELD", 0, Year.builder().build())).convert(2021));
        Assert.assertEquals("string", tidbValueConverters.converter(VARCHAR_OR_VARBINARY_V1_COLUMN, new Field("VARCHAR_OR_VARBINARY_V1_FIELD", 0, SchemaBuilder.string().build())).convert("string"));
        Assert.assertEquals(Short.valueOf("0"), tidbValueConverters.converter(BIT_COLUMN, new Field("BIT_FIELD", 0, Bits.builder(BIT_COLUMN.length()).build())).convert(0));
        Assert.assertEquals("string", tidbValueConverters.converter(VARCHAR_OR_VARBINARY_V2_COLUMN, new Field("VARCHAR_OR_VARBINARY_V2_FIELD", 0, SchemaBuilder.string().build())).convert("string"));
        Assert.assertEquals("{\"key1\": \"value1\"}", tidbValueConverters.converter(JSON_COLUMN, new Field("JSON_FIELD", 0, Json.builder().build())).convert("{\"key1\": \"value1\"}"));
        Schema decimalSchema = SpecialValueDecimal.builder(config.getDecimalMode(), DECIMAL_COLUMN.length(), DECIMAL_COLUMN.scale().get()).build();
        Assert.assertEquals(new BigDecimal("129012.1230000"),
                tidbValueConverters.converter(DECIMAL_COLUMN, new Field("DECIMAL_FIELD", 0, decimalSchema)).convert("129012.1230000"));
        Assert.assertEquals(1, tidbValueConverters.converter(ENUM_COLUMN, new Field("ENUM_FIELD", 0, SchemaBuilder.int32().build())).convert(1));
        Assert.assertEquals(1, tidbValueConverters.converter(SET_COLUMN, new Field("SET_FIELD", 0, SchemaBuilder.int32().build())).convert(1));
        Assert.assertEquals("string", tidbValueConverters.converter(TINYTEXT_OR_TINYBLOB_COLUMN, new Field("TINYTEXT_OR_TINYBLOB_FIELD", 0, SchemaBuilder.string().build())).convert("c3RyaW5n"));
        Assert.assertEquals("string",
                tidbValueConverters.converter(MEDIUMTEXT_OR_MEDIUMBLOB_COLUMN, new Field("MEDIUMTEXT_OR_MEDIUMBLOB_FIELD", 0, SchemaBuilder.string().build())).convert("c3RyaW5n"));
        Assert.assertEquals("string", tidbValueConverters.converter(LONGTEXT_OR_LONGBLOB_COLUMN, new Field("LONGTEXT_OR_LONGBLOB_FIELD", 0, SchemaBuilder.string().build())).convert("c3RyaW5n"));
        Assert.assertEquals("string", tidbValueConverters.converter(TEXT_OR_BLOB_COLUMN, new Field("TEXT_OR_BLOB_FIELD", 0, SchemaBuilder.string().build())).convert("c3RyaW5n"));
        Assert.assertEquals("string", tidbValueConverters.converter(CHAR_OR_BINARY_COLUMN, new Field("CHAR_OR_BINARY_FIELD", 0, SchemaBuilder.string().build())).convert("string"));
    }

    private java.util.Date getDateFromTime(final String time, final String decimalPlaceHolder) {
        return Date.from(
                LocalTime.parse(time, DateTimeFormatter.ofPattern(TidbValueConverters.TIME_FORMAT + decimalPlaceHolder))
                        .atDate(LocalDate.parse("1970-01-01", TidbValueConverters.DATE_FORMATTER))
                        .atZone(ZoneId.of("UTC"))
                        .toInstant()
        );
    }

}
