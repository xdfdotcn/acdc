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

package cn.xdf.acdc.connect.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * HdfsSinkConnector is a Kafka Connect Connector implementation that ingest data from Kafka to
 * HDFS.
 */
@Slf4j
public class HdfsSinkConnector extends SinkConnector {

    public static final String TASK_ID_CONFIG_NAME = "task.id";

    public static final String DEFAULT_TASK_ID = "-1";

    public static final String CONNECTOR_CONFIG_NAME = "name";

    private Map<String, String> configProperties;

    private HdfsSinkConfig config;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) throws ConnectException {
        try {
            configProperties = props;
            config = new HdfsSinkConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start HdfsSinkConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HdfsSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put(TASK_ID_CONFIG_NAME, Integer.toString(i));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() throws ConnectException {

    }

    @Override
    public ConfigDef config() {
        return HdfsSinkConfig.getConfig();
    }
}
