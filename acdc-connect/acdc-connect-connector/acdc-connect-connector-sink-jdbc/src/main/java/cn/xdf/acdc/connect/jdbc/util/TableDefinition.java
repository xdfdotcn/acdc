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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A description of a table.
 */
public class TableDefinition {

    private final TableId id;

    private final Map<String, ColumnDefinition> columnsByName = new LinkedHashMap<>();

    private final Map<String, String> pkColumnNames = new LinkedHashMap<>();

    private final TableType type;

    public TableDefinition(
            final TableId id,
            final Iterable<ColumnDefinition> columns
    ) {
        this(id, columns, TableType.TABLE);
    }

    public TableDefinition(
            final TableId id,
            final Iterable<ColumnDefinition> columns,
            final TableType type
    ) {
        this.id = id;
        this.type = Objects.requireNonNull(type);
        for (ColumnDefinition defn : columns) {
            String columnName = defn.id().name();
            columnsByName.put(
                    columnName,
                    defn.forTable(this.id)
            );
            if (defn.isPrimaryKey()) {
                this.pkColumnNames.put(
                        columnName,
                        columnName
                );
            }
        }
    }

    /**
     * Get table id.
     *
     * @return table id
     */
    public TableId id() {
        return id;
    }

    /**
     * Get table type.
     *
     * @return table type
     */
    public TableType type() {
        return type;
    }

    /**
     * Get column count.
     *
     * @return column count
     */
    public int columnCount() {
        return columnsByName.size();
    }

    /**
     * Get definition for column by name.
     *
     * @param name name
     * @return definition for column
     */
    public ColumnDefinition definitionForColumn(final String name) {
        return columnsByName.get(name);
    }

    /**
     * Get definitions for columns.
     *
     * @return definitions for columns
     */
    public Collection<ColumnDefinition> definitionsForColumns() {
        return columnsByName.values();
    }

    /**
     * Get primary key column names.
     *
     * @return primary key column names
     */
    public Collection<String> primaryKeyColumnNames() {
        return pkColumnNames.values();
    }

    /**
     * Get column names.
     *
     * @return column names
     */
    public Set<String> columnNames() {
        return columnsByName.keySet();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return true;
        }
        if (obj instanceof TableDefinition) {
            TableDefinition that = (TableDefinition) obj;
            return Objects.equals(this.id(), that.id())
                    && Objects.equals(this.type(), that.type())
                    && Objects.equals(this.definitionsForColumns(), that.definitionsForColumns());
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format(
                "Table{name='%s', type=%s columns=%s}",
                id,
                type,
                definitionsForColumns()
        );
    }
}
