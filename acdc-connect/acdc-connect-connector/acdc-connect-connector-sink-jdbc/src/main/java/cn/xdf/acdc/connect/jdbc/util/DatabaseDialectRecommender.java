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

import cn.xdf.acdc.connect.jdbc.dialect.DatabaseDialects;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatabaseDialectRecommender implements ConfigDef.Recommender, ConfigDef.Validator {

    public static final DatabaseDialectRecommender INSTANCE = new DatabaseDialectRecommender();

    private static final List<Object> DIALECT_NAMES;

    static {
        DIALECT_NAMES = new ArrayList<>();
        DIALECT_NAMES.add("");
        DIALECT_NAMES.addAll(DatabaseDialects.registeredDialectNames());
    }

    /**
     * Valid values.
     * @param name name
     * @param parsedConfig parsed config
     * @return dialect names
     */
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
        return DIALECT_NAMES;
    }

    /**
     * Is visible.
     * @param name name
     * @param parsedConfig parsedConfig
     * @return true
     */
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
        return true;
    }

    @Override
    public void ensureValid(final String key, final Object value) {
        if (value != null && !DIALECT_NAMES.contains(value.toString())) {
            throw new ConfigException(key, value, "Invalid enumerator");
        }
    }

    @Override
    public String toString() {
        return DIALECT_NAMES.toString();
    }
}
