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

package cn.xdf.acdc.connect.jdbc;

import cn.xdf.acdc.connect.jdbc.sink.JdbcSinkConfig;
import cn.xdf.acdc.connect.jdbc.sink.JdbcSinkTask;
import cn.xdf.acdc.connect.jdbc.util.Version;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    /**
     * Get task class.
     *
     * @return JdbcSinkTask.class
     */
    public Class<? extends Task> taskClass() {
        return JdbcSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void start(final Map<String, String> props) {
        configProps = props;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return JdbcSinkConfig.CONFIG_DEF;
    }

    @Override
    public Config validate(final Map<String, String> connectorConfigs) {
        // TODO cross-fields validation here: pkFields against the pkMode
        return super.validate(connectorConfigs);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
