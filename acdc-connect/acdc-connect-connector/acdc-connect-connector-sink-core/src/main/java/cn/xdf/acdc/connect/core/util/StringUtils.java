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

package cn.xdf.acdc.connect.core.util;

import org.apache.kafka.connect.data.Schema;

/**
 * General string utilities that are missing from the standard library and may commonly be required by Connector or Task
 * implementations.
 */
public class StringUtils {

    /**
     * Get a string representation of the supplied value that can be included in a log message.
     *
     * @param value the value; may be null
     * @return the loggable string representation
     */
    public static String valueTypeOrNull(final Object value) {
        return value == null ? null : value.getClass().getSimpleName();
    }

    /**
     * Get a string representation of the supplied schema that can be included in a log message.
     *
     * @param schema the schema; may be null
     * @return the loggable string representation
     */
    public static String schemaTypeOrNull(final Schema schema) {
        if (schema == null) {
            return null;
        }
        switch (schema.type()) {
            case STRUCT:
                return "Struct";
            default:
                return schema.type().getName();
        }
    }

}
