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

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.jdbc.dialect.BaseDialectTest;
import cn.xdf.acdc.connect.jdbc.util.DateTimeUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcSinkTaskTest {

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("modified", Timestamp.SCHEMA)
            .build();

    private static final SinkRecord RECORD = new SinkRecord(
            "stub",
            0,
            null,
            null,
            null,
            null,
            0
    );

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private final JdbcDbWriter mockWriter = mock(JdbcDbWriter.class);

    private final SinkTaskContext ctx = mock(SinkTaskContext.class);

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    public void putPropagatesToDbWithAutoCreateAndPkModeKafka() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("pk.mode", "kafka");
        props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
        props.put("delete.enabled", "false");
        props.put("destinations", "atopic");
        props.put("destinations.atopic.fields.whitelist", "firstName,lastName,bool,short,byte,long,float,double,age,modified");
        String timeZoneID = "America/Los_Angeles";
        props.put("db.timezone", timeZoneID);

        JdbcSinkTask task = new JdbcSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        task.start(props);

        final Struct struct = new Struct(SCHEMA)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("bool", true)
                .put("short", (short) 1234)
                .put("byte", (byte) -32)
                .put("long", 12425436L)
                .put("float", (float) 2356.3)
                .put("double", -2436546.56457)
                .put("age", 21)
                .put("modified", new Date(1474661402123L));

        final String topic = "atopic";

        task.put(Collections.singleton(
                new SinkRecord(topic, 1, null, null, SCHEMA, struct, 42)
        ));

        assertEquals(
                1,
                sqliteHelper.select(
                        "SELECT * FROM " + topic,
                        new SqliteHelper.ResultSetReadCallback() {
                            @Override
                            public void read(final ResultSet rs) throws SQLException {
                                assertEquals(topic, rs.getString("kafka_topic"));
                                assertEquals(1, rs.getInt("kafka_partition"));
                                assertEquals(42, rs.getLong("kafka_offset"));
                                assertEquals(struct.getString("firstName"), rs.getString("firstName"));
                                assertEquals(struct.getString("lastName"), rs.getString("lastName"));
                                assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                                assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                                assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                                assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                                assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                                assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
                                assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                                java.sql.Timestamp dbTimestamp = rs.getTimestamp("modified", DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(timeZoneID)));
                                assertEquals(((java.util.Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                            }
                        }
                )
        );
    }

    @Test
    public void putPropagatesToDbWithPkModeRecordValue() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "firstName,lastName");
        props.put("delete.enabled", "false");
        props.put("destinations", "atopic");
        props.put("destinations.atopic.fields.whitelist", "firstName,lastName,bool,short,byte,long,float,double,age,modified");

        JdbcSinkTask task = new JdbcSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        final String topic = "atopic";

        sqliteHelper.createTable(
                "CREATE TABLE " + topic + "("
                        + "    firstName  TEXT,"
                        + "    lastName  TEXT,"
                        + "    age INTEGER,"
                        + "    bool  NUMERIC,"
                        + "    byte  INTEGER,"
                        + "    short INTEGER NULL,"
                        + "    long INTEGER,"
                        + "    float NUMERIC,"
                        + "    double NUMERIC,"
                        + "    bytes BLOB,"
                        + "    modified DATETIME, "
                        + "PRIMARY KEY (firstName, lastName));"
        );

        task.start(props);

        final Struct struct = new Struct(SCHEMA)
                .put("firstName", "Christina")
                .put("lastName", "Brams")
                .put("bool", false)
                .put("byte", (byte) -72)
                .put("long", 8594L)
                .put("double", 3256677.56457d)
                .put("age", 28)
                .put("modified", new Date(1474661402123L));

        task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, SCHEMA, struct, 43)));

        assertEquals(
                1,
                sqliteHelper.select(
                        "SELECT * FROM " + topic + " WHERE firstName='" + struct.getString("firstName")
                                + "' and lastName='" + struct.getString("lastName") + "'",
                        new SqliteHelper.ResultSetReadCallback() {
                            @Override
                            public void read(final ResultSet rs) throws SQLException {
                                assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                                rs.getShort("short");
                                assertTrue(rs.wasNull());
                                assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                                assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                                assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                                rs.getShort("float");
                                assertTrue(rs.wasNull());
                                assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                                java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                        "modified",
                                        DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC))
                                );
                                assertEquals(((java.util.Date) struct.get("modified")).getTime(),
                                        dbTimestamp.getTime());
                            }
                        }
                )
        );
    }

    @Test
    public void retries() throws Exception {
        final int maxRetries = 2;
        final int retryBackoffMs = 1000;

        List<SinkRecord> records = createRecordsList(1);

        doThrow(new RetriableException("cause 1"))
                .doThrow(new RetriableException("cause 2"))
                .doThrow(new RetriableException("cause 3")).when(mockWriter).write(records);
        when(ctx.errantRecordReporter()).thenReturn(null);
        JdbcSinkTask task = new JdbcSinkTask() {
            @Override
            void initWriter() {
                this.setWriter(mockWriter);
            }
        };
        task.initialize(ctx);

        Map<String, String> props = setupBasicProps(maxRetries, retryBackoffMs);
        task.start(props);

        try {
            task.put(records);
            fail();
        } catch (RetriableException expected) {
            assertEquals(RetriableException.class, expected.getClass());
            StringWriter sw = new StringWriter();
            expected.printStackTrace(new PrintWriter(sw));
            System.out.println("Chained exception: " + sw);
        }

        try {
            task.put(records);
            fail();
        } catch (RetriableException expected) {
            assertEquals(RetriableException.class, expected.getClass());
            StringWriter sw = new StringWriter();
            expected.printStackTrace(new PrintWriter(sw));
            System.out.println("Chained exception: " + sw);
        }

        try {
            task.put(records);
            fail();
        } catch (RetriableException e) {
            fail("Non-retriable exception expected");
        } catch (ConnectException expected) {
            assertEquals(ConnectException.class, expected.getClass());
            StringWriter sw = new StringWriter();
            expected.printStackTrace(new PrintWriter(sw));
            System.out.println("Chained exception: " + sw);
        }

    }

    @Test
    public void errorReporting() throws Exception {
        List<SinkRecord> records = createRecordsList(1);

        RetriableException exception = new RetriableException("cause 1");
        doThrow(exception).when(mockWriter).write(records);

        JdbcSinkTask task = new JdbcSinkTask() {
            @Override
            void initWriter() {
                this.setWriter(mockWriter);
            }
        };
        task.initialize(ctx);
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        when(ctx.errantRecordReporter()).thenReturn(reporter);
        when(reporter.report(Matchers.anyObject(), Matchers.anyObject())).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, String> props = setupBasicProps(0, 0);
        task.start(props);
        task.put(records);
    }

    @Test
    public void errorReportingTableAlterOrCreateException() throws Exception {
        List<SinkRecord> records = createRecordsList(1);

        TableAlterOrCreateException exception = new TableAlterOrCreateException("cause 1");
        doThrow(exception).when(mockWriter).write(records);

        JdbcSinkTask task = new JdbcSinkTask() {
            @Override
            void initWriter() {
                this.setWriter(mockWriter);
            }
        };
        task.initialize(ctx);
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        when(ctx.errantRecordReporter()).thenReturn(reporter);
        when(reporter.report(Matchers.anyObject(), Matchers.anyObject())).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, String> props = setupBasicProps(0, 0);
        task.start(props);
        task.put(records);
    }

    @Test
    public void batchErrorReporting() throws Exception {
        final int batchSize = 3;

        List<SinkRecord> records = createRecordsList(batchSize);

        RetriableException exception = new RetriableException("cause 1");
        doThrow(exception).when(mockWriter).write(records);

        JdbcSinkTask task = new JdbcSinkTask() {
            @Override
            void initWriter() {
                this.setWriter(mockWriter);
            }
        };
        task.initialize(ctx);
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        when(ctx.errantRecordReporter()).thenReturn(reporter);
        when(reporter.report(Matchers.anyObject(), Matchers.anyObject())).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, String> props = setupBasicProps(0, 0);
        task.start(props);
        task.put(records);
    }

    private List<SinkRecord> createRecordsList(int batchSize) {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            records.add(RECORD);
        }
        return records;
    }

    private Map<String, String> setupBasicProps(int maxRetries, long retryBackoffMs) {
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConfig.CONNECTION_URL, "stub");
        props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
        props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
        props.put(SinkConfig.DELETE_ENABLED, "false");
        props.put(SinkConfig.DESTINATIONS, BaseDialectTest.DEFAULT_TEST_TABLE_NAME);
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + BaseDialectTest.DEFAULT_TEST_TABLE_NAME + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST, BaseDialectTest.DEFAULT_TEST_FIELD_NAME);
        return props;
    }
}
