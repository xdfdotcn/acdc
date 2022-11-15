package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.ticdc.protocol.Types;
import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@NotThreadSafe
public class TidbDatabaseSchema extends RelationalDatabaseSchema {

    public static final String STRING_EMPTY = "";

    public static final String DOT = ".";

    public static final int MAX_DECIMAL_LENGTH = 38;

    private final RelationalTableFilters filters;

    private final ConcurrentHashMap<TableId, List<Column>> columnsByTableId = new ConcurrentHashMap<>();

    public TidbDatabaseSchema(final TidbConnectorConfig connectorConfig, final TidbValueConverters valueConverter, final TopicSelector<TableId> topicSelector,
                              final SchemaNameAdjuster schemaNameAdjuster) {
        super(connectorConfig, topicSelector, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverter,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getSanitizeFieldNames()),
                true, connectorConfig.getKeyMapper());
        filters = connectorConfig.getTableFilters();
    }

    /**
     * Is table schema changed.
     *
     * @param tableId           table id
     * @param ticdcEventColumns ticdc event columns
     * @return is table schema changed
     */
    public boolean updateTableSchemaIfChanged(final TableId tableId, final List<TicdcEventColumn> ticdcEventColumns) {
        List<Column> columns = ticdcEventColumns.stream().map(ticdcEventColumn -> {
            ColumnEditor columnEditor = Column.editor()
                .name(ticdcEventColumn.getName())
                .nativeType(ticdcEventColumn.getT())
                .type(STRING_EMPTY)
                .charsetName(STRING_EMPTY)
                .charsetNameOfTable(STRING_EMPTY);

            if (columnEditor.nativeType() == Types.DECIMAL) {
                handleWithDecimalType(tableId, ticdcEventColumn, columnEditor);
            }

            return columnEditor.create();
            }
        ).collect(Collectors.toList());

        if (columns.equals(columnsByTableId.get(tableId))) {
            return false;
        }
        columnsByTableId.put(tableId, columns);

        updateTableSchema(tableId, columns);
        return true;
    }

    private void handleWithDecimalType(final TableId tableId, final TicdcEventColumn ticdcEventColumn, final ColumnEditor columnEditor) {
        String value = (String) ticdcEventColumn.getV();
        // Refer to decimal max length :mysql 65, sql server 38
        columnEditor.length(MAX_DECIMAL_LENGTH);
        columnEditor.scale(0);
        if (value != null && value.trim().length() > 0) {
            value = value.trim();
            int indexOfDot = value.indexOf(DOT);
            if (indexOfDot != -1) {
                columnEditor.scale(value.substring(indexOfDot).length() - 1);
            }
        } else if (tables().forTable(tableId) != null) {
            Column column = tables().forTable(tableId).columnWithName(ticdcEventColumn.getName());
            if (column != null) {
                columnEditor.scale(column.scale().get());
            }
        }
    }

    private void updateTableSchema(final TableId tableId, final List<Column> columns) {
        Table table = Table.editor()
            .tableId(tableId)
            .addColumns(columns)
            .create();
        refresh(table);
    }

    /**
     * Is this table id filtered.
     *
     * @param tableId table id
     * @return is filtered
     */
    public boolean isIncludedTable(final TableId tableId) {
        return filters.dataCollectionFilter().isIncluded(tableId);
    }

    /**
     * Get table id by database name and table name.
     *
     * @param catalogName database name
     * @param schemaName  keep empty
     * @param tableName   table name
     * @return table id
     */
    public TableId getTableId(final String catalogName, final String schemaName, final String tableName) {
        return new TableId(catalogName, schemaName, tableName);
    }
}
