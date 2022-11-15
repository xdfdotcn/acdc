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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TableDefinitionBuilder {

    private TableId tableId;

    private Map<String, ColumnDefinitionBuilder> columnBuilders = new HashMap<>();

    /**
     * Get table definition builder by table name.
     *
     * @param tableName table name
     * @return table definition builder
     */
    public TableDefinitionBuilder withTable(final String tableName) {
        tableId = new TableId(null, null, tableName);
        return this;
    }

    /**
     * Get column name.
     *
     * @param columnName column name
     * @return column definition builder
     */
    public ColumnDefinitionBuilder withColumn(final String columnName) {
        return columnBuilders.computeIfAbsent(columnName, ColumnDefinitionBuilder::new);
    }

    /**
     * Get table definition.
     *
     * @return table definition
     */
    public TableDefinition build() {
        return new TableDefinition(
                tableId,
                columnBuilders.values().stream().map(b -> b.buildFor(tableId)).collect(Collectors.toList())
        );
    }

}
