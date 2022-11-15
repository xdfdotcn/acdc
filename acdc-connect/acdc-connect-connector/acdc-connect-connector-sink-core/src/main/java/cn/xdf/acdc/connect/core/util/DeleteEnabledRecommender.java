/*
 * Copyright 2019 Confluent Inc.
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

import cn.xdf.acdc.connect.core.util.config.PrimaryKeyMode;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeleteEnabledRecommender implements ConfigDef.Recommender {

    public static final DeleteEnabledRecommender INSTANCE = new DeleteEnabledRecommender();

    private static final List<Object> ALL_VALUES = Arrays.asList(Boolean.TRUE, Boolean.FALSE);

    private static final List<Object> DISABLED = Collections.singletonList(Boolean.FALSE);

    private static boolean isRecordKeyPKMode(final Map<String, Object> parsedConfig) {
        return PrimaryKeyMode.RECORD_KEY.name()
                .equalsIgnoreCase(String.valueOf(parsedConfig.get(SinkConfig.PK_MODE)));
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
        return isRecordKeyPKMode(parsedConfig) ? ALL_VALUES : DISABLED;
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
        return isRecordKeyPKMode(parsedConfig);
    }

}
