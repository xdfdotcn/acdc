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

package cn.xdf.acdc.connect.hdfs.util;

import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.parquet.Strings;

public class DataUtils {

    /**
     * Get field from struct.
     * @param structOrMap  the struct
     * @param fieldName  the file in the hdfs
     * @return filed
     */
    public static Object getField(final Object structOrMap, final String fieldName) {
        validate(structOrMap, fieldName);

        Object field;
        if (structOrMap instanceof Struct) {
            field = ((Struct) structOrMap).get(fieldName);
        } else if (structOrMap instanceof Map) {
            field = ((Map<?, ?>) structOrMap).get(fieldName);
            if (field == null) {
                throw new DataException(String.format("Unable to find nested field '%s'", fieldName));
            }
            return field;
        } else {
            throw new DataException(String.format(
                "Argument not a Struct or Map. Cannot get field '%s' from %s.",
                fieldName,
                structOrMap
            ));
        }
        if (field == null) {
            throw new DataException(
                String.format("The field '%s' does not exist in %s.", fieldName, structOrMap));
        }
        return field;
    }

    /**
     * Get nested field from struct.
     * @param structOrMap  the struct
     * @param fieldName  the file in the hdfs
     * @return filed
     */
    public static Object getNestedFieldValue(final Object structOrMap, final String fieldName) {
        validate(structOrMap, fieldName);

        try {
            Object innermost = structOrMap;
            // Iterate down to final struct
            for (String name : fieldName.split("\\.")) {
                innermost = getField(innermost, name);
            }
            return innermost;
        } catch (DataException e) {
            throw new DataException(
                String.format("The field '%s' does not exist in %s.", fieldName, structOrMap),
                e
            );
        }
    }

    /**
     * Get nested field from schema.
     * @param schema  the schema
     * @param fieldName  the file in the hdfs
     * @return filed
     */
    public static Field getNestedField(final Schema schema, final String fieldName) {
        validate(schema, fieldName);

        final String[] fieldNames = fieldName.split("\\.");
        try {
            Field innermost = schema.field(fieldNames[0]);
            // Iterate down to final schema
            for (int i = 1; i < fieldNames.length; ++i) {
                innermost = innermost.schema().field(fieldNames[i]);
            }
            return innermost;
        } catch (DataException e) {
            throw new DataException(
                String.format("Unable to get field '%s' from schema %s.", fieldName, schema),
                e
            );
        }
    }

    private static void validate(final Object o, final String fieldName) {
        if (o == null) {
            throw new ConnectException("Attempted to extract a field from a null object.");
        }
        if (Strings.isNullOrEmpty(fieldName)) {
            throw new ConnectException("The field to extract cannot be null or empty.");
        }
    }
}
