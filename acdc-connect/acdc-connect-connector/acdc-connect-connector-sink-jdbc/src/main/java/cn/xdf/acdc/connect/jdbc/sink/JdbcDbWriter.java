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

import cn.xdf.acdc.connect.core.sink.AbstractBufferedRecords;
import cn.xdf.acdc.connect.core.sink.AbstractBufferedWriter;
import cn.xdf.acdc.connect.core.util.ExceptionUtils;
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.util.CachedConnectionProvider;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

@Slf4j
@Getter
public class JdbcDbWriter extends AbstractBufferedWriter<Connection> {

    private final CachedConnectionProvider cachedConnectionProvider;

    private final JdbcSinkConfig config;

    private final DatabaseDialect dbDialect;

    private final DbStructure dbStructure;

    JdbcDbWriter(final JdbcSinkConfig config, final DatabaseDialect dbDialect, final DbStructure dbStructure) {
        super(config);
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.cachedConnectionProvider = connectionProvider(
                config.getConnectionAttempts(),
                config.getConnectionBackoffMs()
        );
    }

    protected CachedConnectionProvider connectionProvider(final int maxConnAttempts, final long retryBackoff) {
        return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                log.info("JdbcDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    @Override
    protected void commit(final Connection connection) {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw ExceptionUtils.parseToFlatMessageRetriableException(e);
        }
    }

    @Override
    protected AbstractBufferedRecords getBufferedRecords(final Connection connection, final String tableName) {
        return new JdbcBufferedRecords(config, new TableId(null, null, tableName), dbDialect, dbStructure, connection);
    }

    @Override
    protected Connection getClient() {
        return cachedConnectionProvider.getConnection();
    }

    @Override
    public void closePartitions(final Collection<TopicPartition> partitions) {

    }

    @Override
    public void close() {
        cachedConnectionProvider.close();
    }
}
