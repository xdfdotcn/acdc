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
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.dialect.SqliteDatabaseDialect;
import cn.xdf.acdc.connect.jdbc.util.ColumnDefinition;
import cn.xdf.acdc.connect.jdbc.util.TableDefinition;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SqliteHelperTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    public void returnTheDatabaseTableInformation() throws SQLException {
        String createEmployees = "CREATE TABLE employees\n"
                + "( employee_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "  last_name VARCHAR NOT NULL,\n"
                + "  first_name VARCHAR,\n"
                + "  hire_date DATE\n"
                + ");";

        String createProducts = "CREATE TABLE products\n"
                + "( product_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "  product_name VARCHAR NOT NULL,\n"
                + "  quantity INTEGER NOT NULL DEFAULT 0\n"
                + ");";

        String createNonPkTable = "CREATE TABLE nonPk (id numeric, response text)";

        sqliteHelper.createTable(createEmployees);
        sqliteHelper.createTable(createProducts);
        sqliteHelper.createTable(createNonPkTable);

        Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSinkConfig.CONNECTION_URL, sqliteHelper.sqliteUri());
        connProps.put(SinkConfig.DESTINATIONS, BaseDialectTest.DEFAULT_TEST_TABLE_NAME);
        connProps.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + BaseDialectTest.DEFAULT_TEST_TABLE_NAME + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST, BaseDialectTest.DEFAULT_TEST_FIELD_NAME);
        JdbcSinkConfig config = new JdbcSinkConfig(connProps);
        DatabaseDialect dialect = new SqliteDatabaseDialect(config);

        final Map<String, TableDefinition> tables = new HashMap<>();
        for (TableId tableId : dialect.tableIds(sqliteHelper.getConnection())) {
            tables.put(tableId.tableName(), dialect.describeTable(sqliteHelper.getConnection(), tableId));
        }

        assertEquals(tables.size(), 3);
        assertTrue(tables.containsKey("employees"));
        assertTrue(tables.containsKey("products"));
        assertTrue(tables.containsKey("nonPk"));

        TableDefinition nonPk = tables.get("nonPk");
        assertEquals(2, nonPk.columnCount());

        ColumnDefinition colDefn = nonPk.definitionForColumn("id");
        assertTrue(colDefn.isOptional());
        assertFalse(colDefn.isPrimaryKey());
        assertEquals(Types.FLOAT, colDefn.type());

        colDefn = nonPk.definitionForColumn("response");
        assertTrue(colDefn.isOptional());
        assertFalse(colDefn.isPrimaryKey());
        assertEquals(Types.VARCHAR, colDefn.type());

        TableDefinition employees = tables.get("employees");
        assertEquals(4, employees.columnCount());

        assertNotNull(employees.definitionForColumn("employee_id"));
        assertFalse(employees.definitionForColumn("employee_id").isOptional());
        assertTrue(employees.definitionForColumn("employee_id").isPrimaryKey());
        assertEquals(Types.INTEGER, employees.definitionForColumn("employee_id").type());
        assertNotNull(employees.definitionForColumn("last_name"));
        assertFalse(employees.definitionForColumn("last_name").isOptional());
        assertFalse(employees.definitionForColumn("last_name").isPrimaryKey());
        assertEquals(Types.VARCHAR, employees.definitionForColumn("last_name").type());
        assertNotNull(employees.definitionForColumn("first_name"));
        assertTrue(employees.definitionForColumn("first_name").isOptional());
        assertFalse(employees.definitionForColumn("first_name").isPrimaryKey());
        assertEquals(Types.VARCHAR, employees.definitionForColumn("first_name").type());
        assertNotNull(employees.definitionForColumn("hire_date"));
        assertTrue(employees.definitionForColumn("hire_date").isOptional());
        assertFalse(employees.definitionForColumn("hire_date").isPrimaryKey());
        // sqlite returns VARCHAR for DATE. why?!
        assertEquals(Types.VARCHAR, employees.definitionForColumn("hire_date").type());
        // assertEquals(columns.get("hire_date").getSqlType(), Types.DATE);

        TableDefinition products = tables.get("products");
        assertEquals(4, employees.columnCount());

        assertNotNull(products.definitionForColumn("product_id"));
        assertFalse(products.definitionForColumn("product_id").isOptional());
        assertTrue(products.definitionForColumn("product_id").isPrimaryKey());
        assertEquals(Types.INTEGER, products.definitionForColumn("product_id").type());
        assertNotNull(products.definitionForColumn("product_name"));
        assertFalse(products.definitionForColumn("product_name").isOptional());
        assertFalse(products.definitionForColumn("product_name").isPrimaryKey());
        assertEquals(Types.VARCHAR, products.definitionForColumn("product_name").type());
        assertNotNull(products.definitionForColumn("quantity"));
        assertFalse(products.definitionForColumn("quantity").isOptional());
        assertFalse(products.definitionForColumn("quantity").isPrimaryKey());
        assertEquals(Types.INTEGER, products.definitionForColumn("quantity").type());
    }
}
