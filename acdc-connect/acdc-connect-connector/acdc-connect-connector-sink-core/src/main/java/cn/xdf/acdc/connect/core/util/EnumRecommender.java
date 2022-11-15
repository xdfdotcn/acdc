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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class EnumRecommender implements ConfigDef.Validator, ConfigDef.Recommender {

    private final List<String> canonicalValues;

    private final Set<String> validValues;

    private EnumRecommender(final List<String> canonicalValues, final Set<String> validValues) {
        this.canonicalValues = canonicalValues;
        this.validValues = validValues;
    }

    /**
     * Factory method for {@link EnumRecommender}.
     *
     * @param enumerators enumerators in the enum recommender
     * @param <E> type of enumerators
     * @return enum recommender instance
     */
    @SafeVarargs
    public static <E> EnumRecommender in(final E... enumerators) {
        final List<String> canonicalValues = new ArrayList<>(enumerators.length);
        final Set<String> validValues = new HashSet<>(enumerators.length * 2);
        for (E e : enumerators) {
            canonicalValues.add(e.toString().toLowerCase());
            validValues.add(e.toString().toUpperCase(Locale.ROOT));
            validValues.add(e.toString().toLowerCase(Locale.ROOT));
        }
        return new EnumRecommender(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(final String key, final Object value) {
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            for (Object v : values) {
                if (v == null) {
                    validate(key, null);
                } else {
                    validate(key, v);
                }
            }
        } else {
            validate(key, value);
        }
    }

    protected void validate(final String key, final Object value) {
        // calling toString on itself because IDE complains if the Object is passed.
        if (value != null && !validValues.contains(value.toString())) {
            throw new ConfigException(key, value, "Invalid enumerator");
        }
    }

    @Override
    public String toString() {
        return canonicalValues.toString();
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
        return new ArrayList<>(canonicalValues);
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
        return true;
    }

}
