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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utilities for configuration properties.
 */
@Slf4j
public class ConfigUtils {

    /**
     * Get the connector's name from the configuration.
     *
     * @param connectorProps the connector properties
     * @return the concatenated string with delimiters
     */
    public static String connectorName(final Map<String, String> connectorProps) {
        Object nameValue = connectorProps.get("name");
        return nameValue != null ? nameValue.toString() : null;
    }

    /**
     * Get the config from the configuration, and split it by specified separator.
     *
     * @param connectorProps the connector properties
     * @param key configuration key
     * @param separator specified separator to split the configuration value
     * @return result in set
     */
    public static Set<String> stringToSet(final Map<String, String> connectorProps, final String key, final String separator) {
        String value = connectorProps.get(key);
        if (Strings.isNullOrEmpty(value)) {
            return new HashSet<>();
        }

        List<String> values = Splitter.on(separator).trimResults().splitToList(value);
        return new HashSet(values);
    }

    /**
     * Get the config from the configuration, and parse it to map.
     * The configuration value is a format like: key_1:value_1,key_2:value_2
     *
     * @param connectorProps the connector properties
     * @param key configuration key
     * @param separator specified separator to split the configuration value
     * @param keyValueSeparator specified separator to split the key and value
     * @return result in map
     */
    public static Map<String, String> stringToMap(final Map<String, String> connectorProps, final String key, final String separator, final String keyValueSeparator) {
        String value = connectorProps.get(key);
        if (Strings.isNullOrEmpty(value)) {
            return new HashMap<>();
        }
        value = value.replaceAll("\\s", "");
        return Splitter.on(separator).withKeyValueSeparator(keyValueSeparator).split(value);
    }

    /**
     * Get a value from configuration.
     * If the key not existed, return the default value.
     *
     * @param connectorProps the connector properties
     * @param key configuration key
     * @param defaultValue default value
     * @return value in configuration or default
     */
    public static String getStringOrDefault(final Map<String, String> connectorProps, final String key, final String defaultValue) {
        String value = connectorProps.get(key);
        return value == null ? defaultValue : value.trim();
    }

    /**
     * Check the value is null or not.
     *
     * @param key configuration key
     * @param value configuration value
     */
    public static void checkNotNull(final String key, final Object value) {
        if (null == value) {
            throw new ConfigException("config key: {}, this config should be set.", key);
        }
    }

    /**
     * Check the value is null or empty or not.
     *
     * @param key configuration key
     * @param value configuration value
     */
    public static void checkNotNullOrEmpty(final String key, final Collection value) {
        if (null == value || value.isEmpty()) {
            throw new ConfigException(key, value);
        }
    }

}
