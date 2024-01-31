package cn.xdf.acdc.devops.service.process.widetable.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public final class RelationalBaseTable extends AbstractTable {
    
    private final String tableName;
    
    private final List<String> fieldNames;
    
    private final List<SqlTypeName> fieldTypes;
    
    private RelDataType rowType;
    
    private RelationalBaseTable(
            final String tableName,
            final List<String> fieldNames,
            final List<SqlTypeName> fieldTypes) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());
            
            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = typeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }
            
            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
        }
        
        return rowType;
    }
    
    /**
     * New a relational base table builder.
     *
     * @param tableName table name
     * @return relational base table builder
     */
    public static Builder newBuilder(final String tableName) {
        return new Builder(tableName);
    }
    
    public static final class Builder {
        
        private final String tableName;
        
        private String filePath;
        
        private final List<String> fieldNames = new ArrayList<>();
        
        private final List<SqlTypeName> fieldTypes = new ArrayList<>();
        
        private Builder(final String tableName) {
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("Table name cannot be null or empty");
            }
            this.tableName = tableName;
        }
        
        /**
         * Add field to the table.
         *
         * @param name field name
         * @param typeName field type
         */
        public void addField(final String name, final SqlTypeName typeName) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Field name cannot be null or empty");
            }
            if (fieldNames.contains(name)) {
                throw new IllegalArgumentException("Field already defined: " + name);
            }
            fieldNames.add(name);
            fieldTypes.add(typeName);
        }
        
        public RelationalBaseTable build() {
            if (fieldNames.isEmpty()) {
                throw new IllegalStateException("Table must have at least one field");
            }
            return new RelationalBaseTable(tableName, fieldNames, fieldTypes);
        }
    }
}

