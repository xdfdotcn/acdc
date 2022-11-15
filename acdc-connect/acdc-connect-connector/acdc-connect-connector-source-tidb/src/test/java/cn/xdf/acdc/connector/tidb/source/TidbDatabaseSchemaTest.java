package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.ticdc.protocol.Types;
import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TidbDatabaseSchemaTest {

    @Test
    public void testUpdateTableSchemaIfChangedShouldUpdateSchemaWithDecimalIsNotNull() {
        TidbDatabaseSchema tidbDatabaseSchema = getTidbDatabaseSchema();
        TableId tableId = new TableId("database_name", null, "table_name");
        List<TicdcEventColumn> columns = new ArrayList<>();
        columns.add(getTicdcEventColumn(Types.BIGINT, "id_column", 1112));
        columns.add(getTicdcEventColumn(Types.DECIMAL, "decimal_column", null));
        Assert.assertTrue(tidbDatabaseSchema.updateTableSchemaIfChanged(tableId, columns));
        Assert.assertFalse(tidbDatabaseSchema.updateTableSchemaIfChanged(tableId, columns));
        columns.clear();
        columns.add(getTicdcEventColumn(Types.BIGINT, "id_column", 1112));
        columns.add(getTicdcEventColumn(Types.DECIMAL, "decimal_column", "123.23"));
        Assert.assertTrue(tidbDatabaseSchema.updateTableSchemaIfChanged(tableId, columns));
        Assert.assertFalse(tidbDatabaseSchema.updateTableSchemaIfChanged(tableId, columns));
        columns.clear();
        columns.add(getTicdcEventColumn(Types.BIGINT, "id_column", 1112));
        columns.add(getTicdcEventColumn(Types.DECIMAL, "decimal_column", null));
        Assert.assertFalse(tidbDatabaseSchema.updateTableSchemaIfChanged(tableId, columns));
        columns.clear();
        columns.add(getTicdcEventColumn(Types.BIGINT, "id_column", 1112));
        columns.add(getTicdcEventColumn(Types.DECIMAL, "decimal_column", " "));
        Assert.assertFalse(tidbDatabaseSchema.updateTableSchemaIfChanged(tableId, columns));
    }

    private TicdcEventColumn getTicdcEventColumn(final int type, final String name, final Object value) {
        TicdcEventColumn column = new TicdcEventColumn();
        column.setT(type);
        column.setName(name);
        column.setV(value);
        return column;
    }

    private TidbDatabaseSchema getTidbDatabaseSchema() {
        TidbConnectorConfig config = TidbConnectorConfigTest.getTidbConnectorConfig("bootstrap", "topic", "groupId", 1);
        TopicSelector<TableId> selector = TopicSelector.defaultSelector(config, (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.catalog(), tableId.table()));
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        TidbValueConverters valueConverterProvider = TidbValueConverters.getValueConverters(config);
        return new TidbDatabaseSchema(config, valueConverterProvider, selector, schemaNameAdjuster);
    }

}
