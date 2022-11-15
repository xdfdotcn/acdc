/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.jdbc.sink;

import cn.xdf.acdc.connect.core.util.config.PrimaryKeyMode;
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.dialect.SqliteDatabaseDialect;
import cn.xdf.acdc.connect.jdbc.util.TableDefinition;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import junit.framework.TestCase;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class JdbcDbWriterTest {

    private static final String TOPIC = "books";

    private static final int PARTITION = 7;

    private static final long OFFSET = 42;

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private JdbcDbWriter writer;

    private DatabaseDialect dialect;

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        if (writer != null) {
            writer.close();
        }
        sqliteHelper.tearDown();
    }

    private JdbcDbWriter newWriter(final Map<String, String> props) {
        final JdbcSinkConfig config = new JdbcSinkConfig(props);
        dialect = new SqliteDatabaseDialect(config);
        final DbStructure dbStructure = new DbStructure(dialect);
        return new JdbcDbWriter(config, dialect, dbStructure);
    }

    @Test
    public void autoCreateWithAutoEvolve() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("auto.evolve", "true");
        props.put("pk.mode", "record_key");
        // assigned name for the primitive key
        props.put("pk.fields", "id");
        props.put("destinations", "books");
        props.put("destinations.books.fields.whitelist", "author,title,year,review");

        writer = newWriter(props);

        Schema keySchema = Schema.INT64_SCHEMA;

        Schema valueSchema1 = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct1 = new Struct(valueSchema1)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito");

        writer.write(Collections.singleton(new SinkRecord(TOPIC, 0, keySchema, 1L, valueSchema1,
                valueStruct1, 0)));

        TableDefinition metadata = dialect.describeTable(writer.getCachedConnectionProvider().getConnection(),
                new TableId(null, null, TOPIC));
        TestCase.assertTrue(metadata.definitionForColumn("id").isPrimaryKey());
        for (Field field : valueSchema1.fields()) {
            Assert.assertNotNull(metadata.definitionForColumn(field.name()));
        }

        Schema valueSchema2 = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                // new field
                .field("year", Schema.OPTIONAL_INT32_SCHEMA)
                // new field
                .field("review", SchemaBuilder.string().defaultValue("").build());

        Struct valueStruct2 = new Struct(valueSchema2)
                .put("author", "Tom Robbins")
                .put("title", "Fierce Invalids");

        writer.write(Collections.singleton(new SinkRecord(TOPIC, 0, keySchema, 2L, valueSchema2, valueStruct2, 0)));

        TableDefinition refreshedMetadata = dialect.describeTable(sqliteHelper.getConnection(), new TableId(null, null, TOPIC));
        TestCase.assertTrue(refreshedMetadata.definitionForColumn("id").isPrimaryKey());
        for (Field field : valueSchema2.fields()) {
            Assert.assertNotNull(refreshedMetadata.definitionForColumn(field.name()));
        }
    }

    @Test(expected = RetriableException.class)
    public void multiInsertWithKafkaPkFailsDueToUniqueConstraint() throws Exception {
        writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, PrimaryKeyMode.KAFKA,
                "");
    }

    @Test
    public void idempotentUpsertWithKafkaPk() throws Exception {
        writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, PrimaryKeyMode.KAFKA,
                "");
    }

    @Test(expected = RetriableException.class)
    public void multiInsertWithRecordKeyPkFailsDueToUniqueConstraint() throws Exception {
        writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT,
                PrimaryKeyMode.RECORD_KEY, "");
    }

    @Test
    public void idempotentUpsertWithRecordKeyPk() throws Exception {
        writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT,
                PrimaryKeyMode.RECORD_KEY, "");
    }

    @Test(expected = RetriableException.class)
    public void multiInsertWithRecordValuePkFailsDueToUniqueConstraint() throws Exception {
        writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT,
                PrimaryKeyMode.RECORD_VALUE, "author,title");
    }

    @Test
    public void idempotentUpsertWithRecordValuePk() throws Exception {
        writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT,
                PrimaryKeyMode.RECORD_VALUE, "author,title");
    }

    private void writeSameRecordTwiceExpectingSingleUpdate(
            final JdbcSinkConfig.InsertMode insertMode,
            final PrimaryKeyMode pkMode,
            final String pkFields
    ) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("pk.mode", pkMode.toString());
        props.put("pk.fields", pkFields);
        props.put("insert.mode", insertMode.toString());
        props.put("delete.enabled", "false");
        props.put("destinations", "books");
        props.put("destinations.books.fields.whitelist", "author,title");

        writer = newWriter(props);

        Schema keySchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.INT64_SCHEMA);

        Struct keyStruct = new Struct(keySchema).put("id", 0L);

        Schema valueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito");

        SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);

        writer.write(Collections.nCopies(2, record));

        Assert.assertEquals(
                1,
                sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        Assert.assertEquals(1, rs.getInt(1));
                    }
                })
        );
    }

    @Test
    public void idempotentDeletes() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("delete.enabled", "true");
        props.put("pk.mode", "record_key");
        props.put("insert.mode", "upsert");
        props.put("destinations", "books");
        props.put("destinations.books.fields.whitelist", "id,author,title");

        writer = newWriter(props);

        Schema keySchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.INT64_SCHEMA);

        Struct keyStruct = new Struct(keySchema).put("id", 0L);

        Schema valueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito")
                .put("__deleted", "false");

        SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);

        writer.write(Collections.nCopies(2, record));
        Schema deleteValueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        Struct deleteValueStruct = new Struct(deleteValueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito")
                .put("__deleted", "true");

        SinkRecord deleteRecord = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, deleteValueSchema, deleteValueStruct, OFFSET);
        writer.write(Collections.nCopies(2, deleteRecord));

        Assert.assertEquals(
                1,
                sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        Assert.assertEquals(0, rs.getInt(1));
                    }
                })
        );
    }

    @Test
    public void insertDeleteSameRecord() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("delete.enabled", "true");
        props.put("pk.mode", "record_key");
        props.put("insert.mode", "upsert");
        props.put("destinations", "books");
        props.put("destinations.books.fields.whitelist", "author,title");

        writer = newWriter(props);

        Schema keySchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.INT64_SCHEMA);

        Struct keyStruct = new Struct(keySchema).put("id", 0L);

        Schema valueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito")
                .put("__deleted", "false");

        SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);

        Schema deleteValueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        Struct deleteValueStruct = new Struct(deleteValueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito")
                .put("__deleted", "true");

        SinkRecord deleteRecord = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, deleteValueSchema, deleteValueStruct, OFFSET);
        writer.write(Collections.singletonList(record));
        writer.write(Collections.singletonList(deleteRecord));

        Assert.assertEquals(
                1,
                sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        Assert.assertEquals(0, rs.getInt(1));
                    }
                })
        );
    }

    @Test
    public void insertDeleteInsertSameRecord() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("delete.enabled", "true");
        props.put("pk.mode", "record_key");
        props.put("insert.mode", "upsert");
        props.put("destinations", "books");
        props.put("destinations.books.fields.whitelist", "id,author,title");
        writer = newWriter(props);

        Schema keySchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.INT64_SCHEMA);

        Struct keyStruct = new Struct(keySchema).put("id", 0L);

        Schema valueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito");

        SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);
        Schema deleteValueSchema = SchemaBuilder.struct()
                .field("author", Schema.STRING_SCHEMA)
                .field("title", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        Struct deleteValueStruct = new Struct(deleteValueSchema)
                .put("author", "Tom Robbins")
                .put("title", "Villa Incognito")
                .put("__deleted", "true");
        SinkRecord deleteRecord = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, deleteValueSchema, deleteValueStruct, OFFSET);
        writer.write(Collections.singletonList(record));
        writer.write(Collections.singletonList(deleteRecord));
        writer.write(Collections.singletonList(record));

        Assert.assertEquals(
                1,
                sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        Assert.assertEquals(1, rs.getInt(1));
                    }
                })
        );
    }

    @Test
    public void sameRecordNTimes() throws Exception {
        String testId = "books";
        String createTable = "CREATE TABLE " + testId + " ("
                + "    the_byte  INTEGER,"
                + "    the_short INTEGER,"
                + "    the_int INTEGER,"
                + "    the_long INTEGER,"
                + "    the_float REAL,"
                + "    the_double REAL,"
                + "    the_bool  INTEGER,"
                + "    the_string TEXT,"
                + "    the_bytes BLOB, "
                + "    the_decimal  NUMERIC,"
                + "    the_date  NUMERIC,"
                + "    the_time  NUMERIC,"
                + "    the_timestamp  NUMERIC"
                + ");";

        sqliteHelper.deleteTable(testId);
        sqliteHelper.createTable(createTable);

        Schema schema = SchemaBuilder.struct().name(testId)
                .field("the_byte", Schema.INT8_SCHEMA)
                .field("the_short", Schema.INT16_SCHEMA)
                .field("the_int", Schema.INT32_SCHEMA)
                .field("the_long", Schema.INT64_SCHEMA)
                .field("the_float", Schema.FLOAT32_SCHEMA)
                .field("the_double", Schema.FLOAT64_SCHEMA)
                .field("the_bool", Schema.BOOLEAN_SCHEMA)
                .field("the_string", Schema.STRING_SCHEMA)
                .field("the_bytes", Schema.BYTES_SCHEMA)
                .field("the_decimal", Decimal.schema(2).schema())
                .field("the_date", Date.SCHEMA)
                .field("the_time", Time.SCHEMA)
                .field("the_timestamp", Timestamp.SCHEMA);

        final java.util.Date instant = new java.util.Date(1474661402123L);

        final Struct struct = new Struct(schema)
                .put("the_byte", (byte) -32)
                .put("the_short", (short) 1234)
                .put("the_int", 42)
                .put("the_long", 12425436L)
                .put("the_float", 2356.3f)
                .put("the_double", -2436546.56457d)
                .put("the_bool", true)
                .put("the_string", "foo")
                .put("the_bytes", new byte[]{-32, 124})
                .put("the_decimal", new BigDecimal("1234.567"))
                .put("the_date", instant)
                .put("the_time", instant)
                .put("the_timestamp", instant);

        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("batch.size", String.valueOf(ThreadLocalRandom.current().nextInt(20, 100)));
        props.put("delete.enabled", "false");
        props.put("pk.mode", "none");
        props.put("auto.create", "true");
        props.put("auto.evolve", "true");
        props.put("insert.mode", "insert");
        props.put("destinations", "books");
        props.put("destinations.books.fields.whitelist", "the_byte,the_short,the_long,the_int,the_float,the_double,the_bool,the_string,the_bytes,the_decimal,the_date,the_time,the_timestamp");

        writer = newWriter(props);
        int numRecords = ThreadLocalRandom.current().nextInt(20, 80);
        writer.write(Collections.nCopies(
                numRecords,
                new SinkRecord("books", 0, null, null, schema, struct, 0)
        ));

        Assert.assertEquals(
                numRecords,
                sqliteHelper.select(
                        "SELECT * FROM " + testId,
                        new SqliteHelper.ResultSetReadCallback() {
                            @Override
                            public void read(final ResultSet rs) throws SQLException {
                                Assert.assertEquals(struct.getInt8("the_byte").byteValue(), rs.getByte("the_byte"));
                                Assert.assertEquals(struct.getInt16("the_short").shortValue(), rs.getShort("the_short"));
                                Assert.assertEquals(struct.getInt32("the_int").intValue(), rs.getInt("the_int"));
                                Assert.assertEquals(struct.getInt64("the_long").longValue(), rs.getLong("the_long"));
                                Assert.assertEquals(struct.getFloat32("the_float"), rs.getFloat("the_float"), 0.01);
                                Assert.assertEquals(struct.getFloat64("the_double"), rs.getDouble("the_double"), 0.01);
                                Assert.assertEquals(struct.getBoolean("the_bool"), rs.getBoolean("the_bool"));
                                Assert.assertEquals(struct.getString("the_string"), rs.getString("the_string"));
                                Assert.assertArrayEquals(struct.getBytes("the_bytes"), rs.getBytes("the_bytes"));
                                Assert.assertEquals(struct.get("the_decimal"), rs.getBigDecimal("the_decimal"));
                                Assert.assertEquals(new java.sql.Date(((java.util.Date) struct.get("the_date")).getTime()), rs.getDate("the_date"));
                                Assert.assertEquals(new java.sql.Time(((java.util.Date) struct.get("the_time")).getTime()), rs.getTime("the_time"));
                                Assert.assertEquals(new java.sql.Timestamp(((java.util.Date) struct.get("the_time")).getTime()), rs.getTimestamp("the_timestamp"));
                            }
                        }
                )
        );
    }
}
