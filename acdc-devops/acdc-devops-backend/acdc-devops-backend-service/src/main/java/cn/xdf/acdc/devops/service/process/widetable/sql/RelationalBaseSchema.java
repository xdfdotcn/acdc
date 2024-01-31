package cn.xdf.acdc.devops.service.process.widetable.sql;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public final class RelationalBaseSchema extends AbstractSchema {
    
    private final String schemaName;
    
    private final Map<String, Table> tableMap;
    
    private RelationalBaseSchema(final String schemaName, final Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }
    
    public String getSchemaName() {
        return schemaName;
    }
    
    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }
    
    @Override
    public Schema snapshot(final SchemaVersion version) {
        return this;
    }
    
    /**
     * New a schema builder.
     *
     * @param schemaName schema name
     * @return schema builder
     */
    public static Builder newBuilder(final String schemaName) {
        return new Builder(schemaName);
    }
    
    public static final class Builder {
        
        private final String schemaName;
        
        private final Map<String, Table> tableMap = new HashMap<>();
        
        private Builder(final String schemaName) {
            if (schemaName == null || schemaName.isEmpty()) {
                throw new IllegalArgumentException("Schema name cannot be null or empty");
            }
            
            this.schemaName = schemaName;
        }
        
        /**
         * Add table to the schema.
         *
         * @param table table
         */
        public void addTable(final RelationalBaseTable table) {
            if (tableMap.containsKey(table.getTableName())) {
                throw new IllegalArgumentException("Table already defined: " + table.getTableName());
            }
            
            tableMap.put(table.getTableName(), table);
            
        }
        
        public RelationalBaseSchema build() {
            return new RelationalBaseSchema(schemaName, tableMap);
        }
    }
}
