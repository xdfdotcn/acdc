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

import java.sql.JDBCType;

public class ColumnDefinitionBuilder {

    private String columnName;

    private String typeName;

    private int jdbcType = JDBCType.INTEGER.ordinal();

    private int displaySize;

    private int precision = 10;

    private int scale;

    private boolean searchable = true;

    private boolean autoIncremented;

    private boolean caseSensitive;

    private boolean currency;

    private boolean signedNumbers;

    private boolean isPrimaryKey;

    private ColumnDefinition.Nullability nullability = ColumnDefinition.Nullability.NULL;

    private ColumnDefinition.Mutability mutability = ColumnDefinition.Mutability.WRITABLE;

    private String classNameForType;

    public ColumnDefinitionBuilder(final String name) {
        this.columnName = name;
    }

    /**
     * Set columnName.
     *
     * @param columnName columnName
     * @return column definition builder
     */
    public ColumnDefinitionBuilder name(final String columnName) {
        this.columnName = columnName;
        return this;
    }

    /**
     * Set typeName jdbcType clazz.
     *
     * @param typeName typeName
     * @param jdbcType jdbcType
     * @param clazz clazz
     * @return column definition builder
     */
    public ColumnDefinitionBuilder type(final String typeName, final JDBCType jdbcType, final Class<?> clazz) {
        typeName(typeName);
        jdbcType(jdbcType);
        classNameForType(clazz != null ? clazz.getName() : null);
        return this;
    }

    /**
     * Set typeName.
     *
     * @param typeName typeName
     * @return column definition builder
     */
    public ColumnDefinitionBuilder typeName(final String typeName) {
        this.typeName = typeName;
        return this;
    }

    /**
     * Set jdbcType.
     *
     * @param type jdbcType
     * @return column definition builder
     */
    public ColumnDefinitionBuilder jdbcType(final JDBCType type) {
        this.jdbcType = type.ordinal();
        return this;
    }

    /**
     * Set classNameForType.
     *
     * @param classNameForType classNameForType
     * @return column definition builder
     */
    public ColumnDefinitionBuilder classNameForType(final String classNameForType) {
        this.classNameForType = classNameForType;
        return this;
    }

    /**
     * Set displaySize.
     *
     * @param size displaySize
     * @return column definition builder
     */
    public ColumnDefinitionBuilder displaySize(int size) {
        this.displaySize = size;
        return this;
    }

    /**
     * Set precision.
     *
     * @param precision precision
     * @return column definition builder
     */
    public ColumnDefinitionBuilder precision(int precision) {
        this.precision = precision;
        return this;
    }

    /**
     * Set scale.
     *
     * @param scale scale
     * @return column definition builder
     */
    public ColumnDefinitionBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    /**
     * Set autoIncremented.
     *
     * @param autoIncremented autoIncremented
     * @return column definition builder
     */
    public ColumnDefinitionBuilder autoIncremented(boolean autoIncremented) {
        this.autoIncremented = autoIncremented;
        return this;
    }

    /**
     * Set caseSensitive.
     *
     * @param caseSensitive caseSensitive
     * @return column definition builder
     */
    public ColumnDefinitionBuilder caseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    /**
     * Set searchable.
     *
     * @param searchable searchable
     * @return column definition builder
     */
    public ColumnDefinitionBuilder searchable(boolean searchable) {
        this.searchable = searchable;
        return this;
    }

    /**
     * Set currency.
     *
     * @param currency currency
     * @return column definition builder
     */
    public ColumnDefinitionBuilder currency(boolean currency) {
        this.currency = currency;
        return this;
    }

    /**
     * Set signedNumbers.
     *
     * @param signedNumbers signedNumbers
     * @return column definition builder
     */
    public ColumnDefinitionBuilder signedNumbers(boolean signedNumbers) {
        this.signedNumbers = signedNumbers;
        return this;
    }

    /**
     * Set isPrimaryKey.
     *
     * @param isPrimaryKey isPrimaryKey
     * @return column definition builder
     */
    public ColumnDefinitionBuilder primaryKey(final boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
        return this;
    }

    /**
     * Set nullable.
     *
     * @param nullable nullable
     * @return column definition builder
     */
    public ColumnDefinitionBuilder nullable(final boolean nullable) {
        return nullability(
                nullable ? ColumnDefinition.Nullability.NULL : ColumnDefinition.Nullability.NOT_NULL
        );
    }

    /**
     * Set nullability.
     *
     * @param nullability nullability
     * @return column definition builder
     */
    public ColumnDefinitionBuilder nullability(final ColumnDefinition.Nullability nullability) {
        this.nullability = nullability;
        return this;
    }

    /**
     * Set mutability.
     * @param mutability mutability
     * @return column definition builder
     */
    public ColumnDefinitionBuilder mutability(final ColumnDefinition.Mutability mutability) {
        this.mutability = mutability;
        return this;
    }

    /**
     * Get column definition by table id.
     *
     * @param tableId table id
     * @return column definition
     */
    public ColumnDefinition buildFor(final TableId tableId) {
        return new ColumnDefinition(
                new ColumnId(tableId, columnName),
                jdbcType,
                typeName,
                classNameForType,
                nullability,
                mutability,
                precision,
                scale,
                signedNumbers,
                displaySize,
                autoIncremented,
                caseSensitive,
                searchable,
                currency,
                isPrimaryKey
        );
    }

}
