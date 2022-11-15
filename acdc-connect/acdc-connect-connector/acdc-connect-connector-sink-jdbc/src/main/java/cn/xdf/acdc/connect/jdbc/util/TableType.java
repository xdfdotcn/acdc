/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableType {

    TABLE("TABLE", "Table"),

    VIEW("VIEW", "View");

    private final String value;

    private final String capitalCase;

    TableType(final String value, final String capitalCase) {
        this.value = value.toUpperCase();
        this.capitalCase = capitalCase;
    }

    /**
     * Get table type by name.
     *
     * @param name name
     * @return table type
     */
    public static TableType get(final String name) {
        String tmpName = name;
        if (name != null) {
            tmpName = name.trim();
        }
        for (TableType method : values()) {
            if (method.toString().equalsIgnoreCase(tmpName)) {
                return method;
            }
        }
        throw new IllegalArgumentException("No matching QuoteMethod found for '" + tmpName + "'");
    }

    /**
     * Parse string collection to table type enum set.
     *
     * @param values values
     * @return table type enum set
     */
    public static EnumSet<TableType> parse(final Collection<String> values) {
        Set<TableType> types = values.stream().map(TableType::get).collect(Collectors.toSet());
        return EnumSet.copyOf(types);
    }

    /**
     * Table type to string.
     *
     * @param types types
     * @param delim delim
     * @return jdbc table type names
     */
    public static String asJdbcTableTypeNames(final EnumSet<TableType> types, final String delim) {
        return types.stream()
                .map(TableType::jdbcName)
                .sorted()
                .collect(Collectors.joining(delim));
    }

    /**
     * Table type to string array.
     *
     * @param types types
     * @return jdbc table type array
     */
    public static String[] asJdbcTableTypeArray(final EnumSet<TableType> types) {
        return types.stream()
                .map(TableType::jdbcName)
                .sorted()
                .collect(Collectors.toList())
                .toArray(new String[types.size()]);
    }

    /**
     * Get capital case.
     *
     * @return capital case
     */
    public String capitalized() {
        return capitalCase;
    }

    /**
     * Get jdbc name.
     *
     * @return jdbc name
     */
    public String jdbcName() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

}
