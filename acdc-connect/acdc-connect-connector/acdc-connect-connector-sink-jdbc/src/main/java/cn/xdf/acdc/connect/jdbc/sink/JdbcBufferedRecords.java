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
import cn.xdf.acdc.connect.core.sink.metadata.FieldsMetadata;
import cn.xdf.acdc.connect.core.sink.metadata.SchemaPair;
import cn.xdf.acdc.connect.core.util.ExceptionUtils;
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.util.ColumnId;
import cn.xdf.acdc.connect.jdbc.util.TableId;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class JdbcBufferedRecords extends AbstractBufferedRecords {

    private final TableId tableId;

    private final JdbcSinkConfig config;

    private final DatabaseDialect dbDialect;

    private final DbStructure dbStructure;

    private final Connection connection;

    private FieldsMetadata fieldsMetadata;

    private PreparedStatement updatePreparedStatement;

    private PreparedStatement deletePreparedStatement;

    private DatabaseDialect.StatementBinder updateStatementBinder;

    private DatabaseDialect.StatementBinder deleteStatementBinder;

    public JdbcBufferedRecords(final JdbcSinkConfig config, final TableId tableId, final DatabaseDialect dbDialect, final DbStructure dbStructure, final Connection connection) {
        super(config);
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
    }

    @Override
    protected void initMetadata(final SinkRecord record) {
        try {
            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(
                    record.keySchema(),
                    record.valueSchema()
            );

            fieldsMetadata = FieldsMetadata.extract(
                    tableId.tableName(),
                    config.getPkMode(),
                    config.getPkFields(),
                    Collections.emptySet(),
                    schemaPair
            );
            dbStructure.createOrAmendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata
            );
            final String insertSql = getInsertSql();
            final String deleteSql = getDeleteSql();
            log.debug(
                    "{} sql: {} deleteSql: {} meta: {}",
                    config.getInsertMode(),
                    insertSql,
                    deleteSql,
                    fieldsMetadata
            );
            close();
            updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
            updateStatementBinder = dbDialect.statementBinder(
                    updatePreparedStatement,
                    config.getPkMode(),
                    schemaPair,
                    fieldsMetadata,
                    dbStructure.tableDefinition(connection, tableId),
                    config.getInsertMode()
            );
            if (config.isDeleteEnabled() && Objects.nonNull(deleteSql)) {
                deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
                deleteStatementBinder = dbDialect.statementBinder(
                        deletePreparedStatement,
                        config.getPkMode(),
                        schemaPair,
                        fieldsMetadata,
                        dbStructure.tableDefinition(connection, tableId),
                        config.getInsertMode()
                );
            }
        } catch (TableAlterOrCreateException e) {
            throw new ConnectException(e);
        } catch (SQLException e) {
            throw ExceptionUtils.parseToFlatMessageRetriableException(e);
        }
    }

    protected void doFlush(final List<SinkRecord> records) {
        log.debug("Flushing {} buffered records", records.size());

        Optional<Long> totalUpdateCount;
        long totalDeleteCount;

        try {
            for (SinkRecord record : records) {
                if (Objects.isNull(record.value()) && Objects.nonNull(deleteStatementBinder)) {
                    deleteStatementBinder.bindRecord(record);
                } else {
                    updateStatementBinder.bindRecord(record);
                }
            }
            totalUpdateCount = executeUpdates();
            totalDeleteCount = executeDeletes();
        } catch (SQLException e) {
            throw ExceptionUtils.parseToFlatMessageRetriableException(e);
        }

        final long expectedCount = updateRecordCount(records);
        log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}", config.getInsertMode(), records.size(), totalUpdateCount, totalDeleteCount);
        if (totalUpdateCount.filter(total -> total != expectedCount).isPresent() && config.getInsertMode() == JdbcSinkConfig.InsertMode.INSERT) {
            throw new ConnectException(String.format(
                    "Update count (%d) did not sum up to total number of records inserted (%d)",
                    totalUpdateCount.get(),
                    expectedCount
            ));
        }
        if (!totalUpdateCount.isPresent()) {
            log.info(
                    "{} records:{} , but no count of the number of rows it affected is available",
                    config.getInsertMode(),
                    records.size()
            );
        }
    }

    /**
     * Execute updates.
     * @return an optional count of all updated rows or an empty optional if no info is available
     */
    private Optional<Long> executeUpdates() throws SQLException {
        Optional<Long> count = Optional.empty();
        for (int updateCount : updatePreparedStatement.executeBatch()) {
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                count = count.isPresent()
                        ? count.map(total -> total + updateCount)
                        : Optional.of((long) updateCount);
            }
        }
        return count;
    }

    private long executeDeletes() throws SQLException {
        long totalDeleteCount = 0;
        if (Objects.nonNull(deletePreparedStatement)) {
            for (int updateCount : deletePreparedStatement.executeBatch()) {
                if (updateCount != Statement.SUCCESS_NO_INFO) {
                    totalDeleteCount += updateCount;
                }
            }
        }
        return totalDeleteCount;
    }

    private long updateRecordCount(final List<SinkRecord> records) {
        return records
                .stream()
                // ignore deletes
                .filter(record -> Objects.nonNull(record.value()) || !config.isDeleteEnabled())
                .count();
    }

    /**
     * Close updatePreparedStatement and deletePreparedStatement.
     */
    public void close() {
        log.debug(
                "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
                updatePreparedStatement,
                deletePreparedStatement
        );

        try {
            if (Objects.nonNull(updatePreparedStatement)) {
                updatePreparedStatement.close();
                updatePreparedStatement = null;
            }
            if (Objects.nonNull(deletePreparedStatement)) {
                deletePreparedStatement.close();
                deletePreparedStatement = null;
            }
        } catch (SQLException e) {
            throw ExceptionUtils.parseToFlatMessageRetriableException(e);
        }
    }

    private String getInsertSql() throws SQLException {
        switch (config.getInsertMode()) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        tableId,
                        asColumns(fieldsMetadata.getKeyFieldNames()),
                        asColumns(fieldsMetadata.getNonKeyFieldNames()),
                        dbStructure.tableDefinition(connection, tableId)
                );
            case UPSERT:
                if (fieldsMetadata.getKeyFieldNames().isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            tableId,
                            asColumns(fieldsMetadata.getKeyFieldNames()),
                            asColumns(fieldsMetadata.getNonKeyFieldNames()),
                            dbStructure.tableDefinition(connection, tableId)
                    );
                } catch (UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            tableId,
                            dbDialect.name()
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                        tableId,
                        asColumns(fieldsMetadata.getKeyFieldNames()),
                        asColumns(fieldsMetadata.getNonKeyFieldNames()),
                        dbStructure.tableDefinition(connection, tableId)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private String getDeleteSql() {
        String sql = null;
        if (config.isDeleteEnabled()) {
            switch (config.getPkMode()) {
                case RECORD_KEY:
                    if (fieldsMetadata.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    try {
                        sql = dbDialect.buildDeleteStatement(
                                tableId,
                                asColumns(fieldsMetadata.getKeyFieldNames())
                        );
                    } catch (UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported with the %s dialect.",
                                tableId,
                                dbDialect.name()
                        ));
                    }
                    break;

                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private Collection<ColumnId> asColumns(final Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
    }

}
