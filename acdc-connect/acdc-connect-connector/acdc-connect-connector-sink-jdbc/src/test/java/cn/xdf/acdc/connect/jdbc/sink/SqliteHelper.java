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

import lombok.Getter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Getter
public final class SqliteHelper {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private final Path dbPath;

    private Connection connection;

    public SqliteHelper(final String testId) {
        dbPath = Paths.get(testId + ".db");
    }

    /**
     * Get sqlite jdbc url.
     *
     * @return jdbc url
     */
    public String sqliteUri() {
        return "jdbc:sqlite:" + dbPath;
    }

    /**
     * Do set up things.
     *
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public void setUp() throws SQLException, IOException {
        Files.deleteIfExists(dbPath);
        connection = DriverManager.getConnection(sqliteUri());
        connection.setAutoCommit(false);
    }

    /**
     * Close connection and delete file metadata.
     *
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public void tearDown() throws SQLException, IOException {
        connection.close();
        Files.deleteIfExists(dbPath);
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
     *
     * @param table table name
     * @throws SQLException SQL exception
     */
    public void deleteTable(final String table) throws SQLException {
        execute("DROP TABLE IF EXISTS " + table);

        //random errors of table not being available happens in the unit tests
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Select count.
     *
     * @param query query
     * @param callback callback
     * @return select count
     * @throws SQLException SQL exception
     */
    public int select(final String query, final SqliteHelper.ResultSetReadCallback callback) throws SQLException {
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
     * Execute the sql.
     *
     * @param sql sql
     * @throws SQLException SQL exception
     */
    public void execute(final String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            connection.commit();
        }
    }

    public interface ResultSetReadCallback {

        /**
         * Read result data set.
         *
         * @param rs result set
         * @throws SQLException SQL exception
         */
        void read(ResultSet rs) throws SQLException;
    }

}
