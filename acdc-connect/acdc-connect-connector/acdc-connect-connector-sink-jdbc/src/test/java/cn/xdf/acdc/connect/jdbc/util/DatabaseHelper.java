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

package cn.xdf.acdc.connect.jdbc.util;

import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.dialect.DropOptions;
import lombok.Getter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Getter
public final class DatabaseHelper {

    private final DatabaseDialect dialect;

    private Connection connection;

    public DatabaseHelper(final DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Do set up things.
     *
     * @throws SQLException SQL exception
     */
    public void setUp() throws SQLException {
        connection = dialect.getConnection();
    }

    /**
     * Close connection and dialect.
     *
     * @throws SQLException SQL exception
     */
    public void tearDown() throws SQLException {
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            if (dialect != null) {
                dialect.close();
            }
        }
    }

    /**
     * Execte create table sql.
     *
     * @param createSql createSql
     * @throws SQLException SQL exception
     */
    public void createTable(final String createSql) throws SQLException {
        execute(createSql);
    }

    /**
     * Execute drop table.
     * @param tableId tableId
     * @throws SQLException SQL exception
     */
    public void deleteTable(final TableId tableId) throws SQLException {
        deleteTable(tableId, new DropOptions().setIfExists(true));
    }

    /**
     * Execute drop table.
     *
     * @param tableId tableId
     * @param options options
     * @throws SQLException SQL exception
     */
    public void deleteTable(final TableId tableId, final DropOptions options) throws SQLException {
        String sql = dialect.buildDropTableStatement(tableId, options);
        execute(sql);

        //random errors of table not being available happens in the unit tests
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute query sql.
     *
     * @param query query sql
     * @param callback result set read callback
     * @return select count
     * @throws SQLException SQL exception
     */
    public int select(final String query, final DatabaseHelper.ResultSetReadCallback callback) throws SQLException {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Execute sql.
     *
     * @param sql sql
     * @throws SQLException SQL exception
     */
    public void execute(final String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    public interface ResultSetReadCallback {

        /**
         * Read result set.
         *
         * @param rs ResultSet
         * @throws SQLException SQL exception
         */
        void read(ResultSet rs) throws SQLException;
    }

}
