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

import cn.xdf.acdc.connect.core.sink.metadata.FieldsMetadata;
import cn.xdf.acdc.connect.core.sink.metadata.SchemaPair;
import cn.xdf.acdc.connect.core.util.config.PrimaryKeyMode;
import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialect;
import cn.xdf.acdc.connect.jdbc.util.ColumnDefinition;
import cn.xdf.acdc.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

public class PreparedStatementBinder implements DatabaseDialect.StatementBinder {

    private final PrimaryKeyMode pkMode;

    private final PreparedStatement statement;

    private final SchemaPair schemaPair;

    private final FieldsMetadata fieldsMetadata;

    private final JdbcSinkConfig.InsertMode insertMode;

    private final DatabaseDialect dialect;

    private final TableDefinition tabDef;

    /**
     * Build this PreparedStatementBinder.
     *
     * @param dialect        dialect
     * @param statement      statement
     * @param pkMode         pkMode
     * @param schemaPair     schemaPair
     * @param fieldsMetadata fieldsMetadata
     * @param insertMode     insertMode
     * @deprecated use {@link #PreparedStatementBinder(DatabaseDialect, PreparedStatement, PrimaryKeyMode, SchemaPair,
     * FieldsMetadata, TableDefinition, JdbcSinkConfig.InsertMode)}
     */
    @Deprecated
    public PreparedStatementBinder(
            final DatabaseDialect dialect,
            final PreparedStatement statement,
            final PrimaryKeyMode pkMode,
            final SchemaPair schemaPair,
            final FieldsMetadata fieldsMetadata,
            final JdbcSinkConfig.InsertMode insertMode
    ) {
        this(
                dialect,
                statement,
                pkMode,
                schemaPair,
                fieldsMetadata,
                null,
                insertMode
        );
    }

    public PreparedStatementBinder(
            final DatabaseDialect dialect,
            final PreparedStatement statement,
            final PrimaryKeyMode pkMode,
            final SchemaPair schemaPair,
            final FieldsMetadata fieldsMetadata,
            final TableDefinition tabDef,
            final JdbcSinkConfig.InsertMode insertMode
    ) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.statement = statement;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
    }

    @Override
    public void bindRecord(final SinkRecord record) throws SQLException {
        final Struct valueStruct = (Struct) record.value();
        final boolean isDelete = Objects.isNull(valueStruct);
        // Assumption: the relevant SQL has placeholders for keyFieldNames first followed by
        //             nonKeyFieldNames, in iteration order for all INSERT/ UPSERT queries
        //             the relevant SQL has placeholders for keyFieldNames,
        //             in iteration order for all DELETE queries
        //             the relevant SQL has placeholders for nonKeyFieldNames first followed by
        //             keyFieldNames, in iteration order for all UPDATE queries

        int index = 1;
        if (isDelete) {
            bindKeyFields(record, index);
        } else {
            switch (insertMode) {
                case INSERT:
                case UPSERT:
                    index = bindKeyFields(record, index);
                    bindNonKeyFields(record, valueStruct, index);
                    break;

                case UPDATE:
                    index = bindNonKeyFields(record, valueStruct, index);
                    bindKeyFields(record, index);
                    break;
                default:
                    throw new AssertionError();

            }
        }
        statement.addBatch();
    }

    protected int bindKeyFields(final SinkRecord record, final int index) throws SQLException {
        int tmpIndex = index;
        switch (pkMode) {
            case NONE:
                if (!fieldsMetadata.getKeyFieldNames().isEmpty()) {
                    throw new AssertionError();
                }
                break;

            case KAFKA:
                assert fieldsMetadata.getKeyFieldNames().size() == 3;
                bindField(tmpIndex++, Schema.STRING_SCHEMA, record.topic(),
                        JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(0));
                bindField(tmpIndex++, Schema.INT32_SCHEMA, record.kafkaPartition(),
                        JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(1));
                bindField(tmpIndex++, Schema.INT64_SCHEMA, record.kafkaOffset(),
                        JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(2));
                break;

            case RECORD_KEY:
                if (schemaPair.getKeySchema().type().isPrimitive()) {
                    assert fieldsMetadata.getKeyFieldNames().size() == 1;
                    bindField(tmpIndex++, schemaPair.getKeySchema(), record.key(),
                            fieldsMetadata.getKeyFieldNames().iterator().next());
                } else {
                    for (String fieldName : fieldsMetadata.getKeyFieldNames()) {
                        final Field field = schemaPair.getKeySchema().field(fieldName);
                        bindField(tmpIndex++, field.schema(), ((Struct) record.key()).get(field), fieldName);
                    }
                }
                break;

            case RECORD_VALUE:
                for (String fieldName : fieldsMetadata.getKeyFieldNames()) {
                    final Field field = schemaPair.getValueSchema().field(fieldName);
                    bindField(tmpIndex++, field.schema(), ((Struct) record.value()).get(field), fieldName);
                }
                break;

            default:
                throw new ConnectException("Unknown primary key mode: " + pkMode);
        }
        return tmpIndex;
    }

    protected int bindNonKeyFields(
            final SinkRecord record,
            final Struct valueStruct,
            final int index
    ) throws SQLException {
        int tmpIndex = index;
        for (final String fieldName : fieldsMetadata.getNonKeyFieldNames()) {
            final Field field = record.valueSchema().field(fieldName);
            bindField(tmpIndex++, field.schema(), valueStruct.get(field), fieldName);
        }
        return tmpIndex;
    }

    protected void bindField(final int index, final Schema schema, final Object value, final String fieldName)
            throws SQLException {
        ColumnDefinition colDef = tabDef == null ? null : tabDef.definitionForColumn(fieldName);
        dialect.bindField(statement, index, schema, value, colDef);
    }
}
