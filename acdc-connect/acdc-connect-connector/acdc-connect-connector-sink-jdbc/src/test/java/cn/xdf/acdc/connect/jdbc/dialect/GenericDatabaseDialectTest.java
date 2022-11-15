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

package cn.xdf.acdc.connect.jdbc.dialect;

import cn.xdf.acdc.connect.core.sink.metadata.SinkRecordField;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.jdbc.sink.JdbcSinkConfig;
import cn.xdf.acdc.connect.jdbc.sink.SqliteHelper;
import cn.xdf.acdc.connect.jdbc.util.ColumnDefinition;
import cn.xdf.acdc.connect.jdbc.util.ColumnId;
import cn.xdf.acdc.connect.jdbc.util.ConnectionProvider;
import cn.xdf.acdc.connect.jdbc.util.EmbeddedDerby;
import cn.xdf.acdc.connect.jdbc.util.ExpressionBuilder;
import cn.xdf.acdc.connect.jdbc.util.IdentifierRules;
import cn.xdf.acdc.connect.jdbc.util.QuoteMethod;
import cn.xdf.acdc.connect.jdbc.util.TableDefinition;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.base.Joiner;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GenericDatabaseDialectTest extends BaseDialectTest<GenericDatabaseDialect> {

    public static final Set<String> TABLE_TYPES = Collections.singleton("TABLE");

    public static final Set<String> VIEW_TYPES = Collections.singleton("VIEW");

    public static final Set<String> ALL_TABLE_TYPES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("TABLE", "VIEW"))
    );

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private Map<String, String> connProps;

    private JdbcSinkConfig sinkConfig;

    private EmbeddedDerby db;

    private ConnectionProvider connectionProvider;

    private Connection conn;

    @Before
    public void setup() throws Exception {
        db = new EmbeddedDerby();
        connProps = new HashMap<>();
        connProps.put(JdbcSinkConfig.CONNECTION_URL, db.getUrl());
        connProps.put(SinkConfig.DESTINATIONS, DEFAULT_TEST_TABLE_NAME);
        connProps.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + DEFAULT_TEST_TABLE_NAME + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST, DEFAULT_TEST_FIELD_NAME);
        newDialectFor(null, null);
        super.setup();
        connectionProvider = dialect;
        conn = connectionProvider.getConnection();
    }

    @After
    public void cleanup() throws Exception {
        connectionProvider.close();
        conn.close();
        db.close();
        db.dropDatabase();
    }

    @Override
    protected GenericDatabaseDialect createDialect() {
        return new GenericDatabaseDialect(sourceConfigWithUrl(db.getUrl()));
    }

    protected GenericDatabaseDialect createDialect(final AbstractConfig config) {
        return new GenericDatabaseDialect(config);
    }

    protected GenericDatabaseDialect newDialectFor(
            final Set<String> tableTypes,
            final String schemaPattern
    ) {
        if (schemaPattern != null) {
            connProps.put(JdbcSinkConfig.SCHEMA_PATTERN_CONFIG, schemaPattern);
        } else {
            connProps.remove(JdbcSinkConfig.SCHEMA_PATTERN_CONFIG);
        }
        if (tableTypes != null) {
            connProps.put(JdbcSinkConfig.TABLE_TYPE_CONFIG, Joiner.on(",").join(tableTypes));
        } else {
            connProps.remove(JdbcSinkConfig.TABLE_TYPE_CONFIG);
        }
        sinkConfig = new JdbcSinkConfig(connProps);
        dialect = createDialect(sinkConfig);
        return dialect;
    }

    protected GenericDatabaseDialect newSinkDialectFor(final Set<String> tableTypes) {
        assertNotNull(tableTypes);
        assertFalse(tableTypes.isEmpty());
        connProps.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, Joiner.on(",").join(tableTypes));
        sinkConfig = new JdbcSinkConfig(connProps);
        dialect = createDialect(sinkConfig);
        assertTrue(dialect.getTableTypes().containsAll(tableTypes));
        return dialect;
    }

    @Test
    public void testDialectForSinkConnectorWithTablesOnly() throws Exception {
        newSinkDialectFor(TABLE_TYPES);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testDialectForSinkConnectorWithViewsOnly() throws Exception {
        newSinkDialectFor(VIEW_TYPES);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testDialectForSinkConnectorWithTablesAndViews() throws Exception {
        newSinkDialectFor(ALL_TABLE_TYPES);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testGetTablesEmpty() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testGetTablesSingle() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("test", "id", "INT");
        TableId test = new TableId(null, "APP", "test");
        assertEquals(Arrays.asList(test), dialect.tableIds(conn));
    }

    @Test
    public void testFindTablesWithKnownTableType() throws Exception {
        Set<String> types = Collections.singleton("TABLE");
        newDialectFor(types, null);
        db.createTable("test", "id", "INT");
        TableId test = new TableId(null, "APP", "test");
        assertEquals(Arrays.asList(test), dialect.tableIds(conn));
    }

    @Test
    public void testNotFindTablesWithUnknownTableType() throws Exception {
        newDialectFor(Collections.singleton("view"), null);
        db.createTable("test", "id", "INT");
        assertEquals(Arrays.asList(), dialect.tableIds(conn));
    }

    @Test
    public void testGetTablesMany() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("test", "id", "INT");
        db.createTable("foo", "id", "INT", "bar", "VARCHAR(20)");
        db.createTable("zab", "id", "INT");
        db.createView("fooview", "foo", "id", "bar");
        TableId test = new TableId(null, "APP", "test");
        TableId foo = new TableId(null, "APP", "foo");
        TableId zab = new TableId(null, "APP", "zab");
        TableId vfoo = new TableId(null, "APP", "fooview");

        // Does not contain views
        assertEquals(new HashSet<>(Arrays.asList(test, foo, zab)), new HashSet<>(dialect.tableIds(conn)));

        newDialectFor(ALL_TABLE_TYPES, null);
        assertEquals(new HashSet<>(Arrays.asList(test, foo, zab, vfoo)), new HashSet<>(dialect.tableIds(conn)));
    }

    @Test
    public void testBuildCreateTableStatement() {
        newDialectFor(TABLE_TYPES, null);
        assertEquals(
                "INSERT INTO \"myTable\"(\"id1\",\"id2\",\"columnA\",\"columnB\",\"columnC\",\"columnD\") VALUES(?,?,?,?,?,?)",
                dialect.buildInsertStatement(tableId, pkColumns, columnsAtoD));
    }

    @Test
    public void testBuildDeleteStatement() {
        newDialectFor(TABLE_TYPES, null);
        assertEquals(
                "DELETE FROM \"myTable\" WHERE \"id1\" = ? AND \"id2\" = ?",
                dialect.buildDeleteStatement(tableId, pkColumns)
        );
    }

    protected void assertTableNames(
            final Set<String> tableTypes,
            final String schemaPattern,
            final TableId... expectedTableIds
    ) throws Exception {
        newDialectFor(tableTypes, schemaPattern);
        Collection<TableId> ids = dialect.tableIds(db.getConnection());
        for (TableId expectedTableId : expectedTableIds) {
            assertTrue(ids.contains(expectedTableId));
        }
        assertEquals(expectedTableIds.length, ids.size());
    }

    @Test
    public void testDescribeTableOnEmptyDb() throws SQLException {
        TableId someTable = new TableId(null, "APP", "some_table");
        TableDefinition defn = dialect.describeTable(db.getConnection(), someTable);
        assertNull(defn);
    }

    @Test
    public void testDescribeTable() throws SQLException {
        TableId tableId = new TableId(null, "APP", "x");
        db.createTable("x",
                "id", "INTEGER PRIMARY KEY",
                "name", "VARCHAR(255) not null",
                "optional_age", "INTEGER");
        TableDefinition defn = dialect.describeTable(db.getConnection(), tableId);
        assertEquals(tableId, defn.id());
        ColumnDefinition columnDefn = defn.definitionForColumn("id");
        assertEquals("INTEGER", columnDefn.typeName());
        assertEquals(Types.INTEGER, columnDefn.type());
        assertEquals(true, columnDefn.isPrimaryKey());
        assertEquals(false, columnDefn.isOptional());

        columnDefn = defn.definitionForColumn("name");
        assertEquals("VARCHAR", columnDefn.typeName());
        assertEquals(Types.VARCHAR, columnDefn.type());
        assertEquals(false, columnDefn.isPrimaryKey());
        assertEquals(false, columnDefn.isOptional());

        columnDefn = defn.definitionForColumn("optional_age");
        assertEquals("INTEGER", columnDefn.typeName());
        assertEquals(Types.INTEGER, columnDefn.type());
        assertEquals(false, columnDefn.isPrimaryKey());
        assertEquals(true, columnDefn.isOptional());
    }

    @Test
    public void testDescribeColumns() throws Exception {
        // Normal case
        db.createTable("test", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY", "bar", "INTEGER");
        TableId test = new TableId(null, "APP", "test");
        ColumnId id = new ColumnId(test, "id");
        ColumnId bar = new ColumnId(test, "bar");
        Map<ColumnId, ColumnDefinition> defns = dialect
                .describeColumns(db.getConnection(), "test", null);
        assertTrue(defns.get(id).isAutoIncrement());
        assertFalse(defns.get(bar).isAutoIncrement());
        assertFalse(defns.get(id).isOptional());
        assertTrue(defns.get(bar).isOptional());

        // No auto increment
        db.createTable("none", "id", "INTEGER", "bar", "INTEGER");
        TableId none = new TableId(null, "APP", "none");
        id = new ColumnId(none, "id");
        bar = new ColumnId(none, "bar");
        defns = dialect.describeColumns(db.getConnection(), "none", null);
        assertFalse(defns.get(id).isAutoIncrement());
        assertFalse(defns.get(bar).isAutoIncrement());
        assertTrue(defns.get(id).isOptional());
        assertTrue(defns.get(bar).isOptional());

        // We can't check multiple columns because Derby ties auto increment to identity and
        // disallows multiple auto increment columns. This is probably ok, multiple auto increment
        // columns should be very unusual anyway

        // Normal case, mixed in and not the first column
        db.createTable("mixed", "foo", "INTEGER", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY",
                "bar", "INTEGER");
        TableId mixed = new TableId(null, "APP", "mixed");
        ColumnId foo = new ColumnId(mixed, "foo");
        id = new ColumnId(mixed, "id");
        bar = new ColumnId(mixed, "bar");
        defns = dialect.describeColumns(db.getConnection(), "mixed", null);
        assertFalse(defns.get(foo).isAutoIncrement());
        assertTrue(defns.get(id).isAutoIncrement());
        assertFalse(defns.get(bar).isAutoIncrement());

        // Derby does not seem to allow null
        db.createTable("tstest", "ts", "TIMESTAMP NOT NULL", "tsdefault", "TIMESTAMP", "tsnull",
                "TIMESTAMP DEFAULT NULL");
        TableId tstest = new TableId(null, "APP", "tstest");
        ColumnId ts = new ColumnId(tstest, "ts");
        ColumnId tsdefault = new ColumnId(tstest, "tsdefault");
        ColumnId tsnull = new ColumnId(tstest, "tsnull");

        defns = dialect.describeColumns(db.getConnection(), "tstest", null);
        assertFalse(defns.get(ts).isOptional());
        // The default for TIMESTAMP columns can vary between databases, but for Derby it is nullable
        assertTrue(defns.get(tsdefault).isOptional());
        assertTrue(defns.get(tsnull).isOptional());
    }

    @Test(expected = ConnectException.class)
    public void shouldBuildCreateQueryStatement() {
        dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    }

    @Test(expected = ConnectException.class)
    public void shouldBuildAlterTableStatement() {
        dialect.buildAlterTable(tableId, sinkRecordFields);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBuildUpsertStatement() {
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    }

    @Test
    public void formatColumnValue() {
        verifyFormatColumnValue("42", Schema.INT8_SCHEMA, (byte) 42);
        verifyFormatColumnValue("42", Schema.INT16_SCHEMA, (short) 42);
        verifyFormatColumnValue("42", Schema.INT32_SCHEMA, 42);
        verifyFormatColumnValue("42", Schema.INT64_SCHEMA, 42L);
        verifyFormatColumnValue("42.5", Schema.FLOAT32_SCHEMA, 42.5f);
        verifyFormatColumnValue("42.5", Schema.FLOAT64_SCHEMA, 42.5d);
        verifyFormatColumnValue("0", Schema.BOOLEAN_SCHEMA, false);
        verifyFormatColumnValue("1", Schema.BOOLEAN_SCHEMA, true);
        verifyFormatColumnValue("'quoteit'", Schema.STRING_SCHEMA, "quoteit");
        verifyFormatColumnValue("x'2A'", Schema.BYTES_SCHEMA, new byte[]{42});

        verifyFormatColumnValue("42.42", Decimal.schema(2), new BigDecimal("42.42"));

        final java.util.Date instant = new java.util.Date(1474661402123L);
        verifyFormatColumnValue("'2016-09-23'", Date.SCHEMA, instant);
        verifyFormatColumnValue("'20:10:02.123'", Time.SCHEMA, instant);
        verifyFormatColumnValue("'2016-09-23 20:10:02.123'", Timestamp.SCHEMA, instant);
    }

    private void verifyFormatColumnValue(final String expected, final Schema schema, final Object value) {
        GenericDatabaseDialect dialect = dummyDialect();
        ExpressionBuilder builder = dialect.expressionBuilder();
        dialect.formatColumnValue(builder, schema.name(), schema.parameters(), schema.type(), value);
        assertEquals(expected, builder.toString());
    }

    private void verifyWriteColumnSpec(final String expected, final SinkRecordField field) {
        GenericDatabaseDialect dialect = dummyDialect();
        ExpressionBuilder builder = dialect.expressionBuilder();
        if (quoteIdentfiiers != null) {
            builder.setQuoteIdentifiers(quoteIdentfiiers);
        }
        dialect.writeColumnSpec(builder, field);
        assertEquals(expected, builder.toString());
    }

    private GenericDatabaseDialect dummyDialect() {
        IdentifierRules rules = new IdentifierRules(",", "`", "`");
        return new GenericDatabaseDialect(sinkConfig, rules) {
            @Override
            protected String getSqlType(final SinkRecordField f) {
                return "DUMMY";
            }
        };
    }

    @Test
    public void writeColumnSpec() {
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42", new SinkRecordField(
                SchemaBuilder.int32().defaultValue(42).build(), "foo", true));
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42",
                new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "foo", false));
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42",
                new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", true));

        quoteIdentfiiers = QuoteMethod.NEVER;
        verifyWriteColumnSpec("foo DUMMY DEFAULT 42",
                new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", false));
        verifyWriteColumnSpec("foo DUMMY NOT NULL", new SinkRecordField(Schema.INT32_SCHEMA, "foo", true));
        verifyWriteColumnSpec("foo DUMMY NOT NULL", new SinkRecordField(Schema.INT32_SCHEMA, "foo", false));
        verifyWriteColumnSpec("foo DUMMY NOT NULL", new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", true));
        verifyWriteColumnSpec("foo DUMMY NULL", new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", false));
    }

    @Test
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
                "jdbc:acme:db/foo:100?key1=value1&key2=value2&key3=value3&&other=value",
                "jdbc:acme:db/foo:100?key1=value1&key2=value2&key3=value3&&other=value"

        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
                "jdbc:acme:db/foo:100?password=secret&key1=value1&key2=value2&key3=value3&"
                        + "user=smith&password=secret&other=value",
                "jdbc:acme:db/foo:100?password=****&key1=value1&key2=value2&key3=value3&"
                        + "user=smith&password=****&other=value"
        );
    }

    @Test
    public void shouldSanitizeUrlWithManyPasswordVariationsInUrlProperties() {
        assertSanitizedUrl(
                "jdbc:acme:db/foo:100?"
                        + "javax.net.ssl.keyStorePassword=secret2&"
                        + "password=secret&"
                        // incorrect parameter before a non-secret
                        + "password&"
                        + "key1=value1&"
                        + "key2=value2&"
                        + "key3=value3&"
                        + "passworNotSanitized=not-secret&"
                        + "passwordShouldBeSanitized=value3&"
                        + "javax.net.ssl.trustStorePassword=superSecret&"
                        + "user=smith&"
                        + "Password=secret&"
                        + "other=value",
                "jdbc:acme:db/foo:100?"
                        + "javax.net.ssl.keyStorePassword=****&"
                        + "password=****&"
                        + "password&"
                        + "key1=value1&"
                        + "key2=value2&"
                        + "key3=value3&"
                        + "passworNotSanitized=not-secret&"
                        + "passwordShouldBeSanitized=****&"
                        + "javax.net.ssl.trustStorePassword=****&"
                        + "user=smith&"
                        + "Password=****&"
                        + "other=value"
        );
    }

    @Test
    public void shouldAddExtraProperties() {
        // When adding extra properties with the 'connection.' prefix
        connProps.put("connection.oracle.something", "somethingValue");
        connProps.put("connection.oracle.else", "elseValue");
        connProps.put("connection.foo", "bar");
        // and some other extra properties not prefixed with 'connection.'
        connProps.put("foo2", "bar2");
        sinkConfig = new JdbcSinkConfig(connProps);
        dialect = createDialect(sinkConfig);
        // and the dialect computes the connection properties
        Properties props = new Properties();
        Properties modified = dialect.addConnectionProperties(props);
        // then the resulting properties
        // should be the same properties object as what was passed in
        assertSame(props, modified);
        // should include props that began with 'connection.' but without prefix
        assertEquals("somethingValue", modified.get("oracle.something"));
        assertEquals("elseValue", modified.get("oracle.else"));
        assertEquals("bar", modified.get("foo"));
        // should not include any 'connection.*' properties defined by the connector
        assertFalse(modified.containsKey("url"));
        assertFalse(modified.containsKey("password"));
        assertFalse(modified.containsKey("connection.url"));
        assertFalse(modified.containsKey("connection.password"));
        // should not include the prefixed props
        assertFalse(modified.containsKey("connection.oracle.something"));
        assertFalse(modified.containsKey("connection.oracle.else"));
        assertFalse(modified.containsKey("connection.foo"));
        // should not include props not prefixed with 'connection.'
        assertFalse(modified.containsKey("foo2"));
        assertFalse(modified.containsKey("connection.foo2"));
    }

}
