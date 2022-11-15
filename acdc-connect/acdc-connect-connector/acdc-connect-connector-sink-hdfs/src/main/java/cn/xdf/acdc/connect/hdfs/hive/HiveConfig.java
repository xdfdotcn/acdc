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

package cn.xdf.acdc.connect.hdfs.hive;

import cn.xdf.acdc.connect.hdfs.common.ComposableConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HiveConfig extends AbstractConfig implements ComposableConfig {

    // Hive group
    public static final String HIVE_INTEGRATION_CONFIG = "hive.integration";

    public static final String HIVE_INTEGRATION_DOC =
        "Configuration indicating whether to integrate with Hive when running the connector.";

    public static final boolean HIVE_INTEGRATION_DEFAULT = false;

    public static final String HIVE_INTEGRATION_DISPLAY = "Hive Integration";

    public static final String HIVE_METASTORE_URIS_CONFIG = "hive.metastore.uris";

    public static final String
        HIVE_METASTORE_URIS_DOC =
        "The Hive metastore URIs, can be IP address or fully-qualified domain name and port of the "
            + "metastore host.";

    public static final String HIVE_METASTORE_URIS_DEFAULT = "";

    public static final String HIVE_METASTORE_URIS_DISPLAY = "Hive Metastore URIs";

    public static final String HIVE_CONF_DIR_CONFIG = "hive.conf.dir";

    public static final String HIVE_CONF_DIR_DOC = "Hive configuration directory";

    public static final String HIVE_CONF_DIR_DEFAULT = "";

    public static final String HIVE_CONF_DIR_DISPLAY = "Hive configuration directory.";

    public static final String HIVE_HOME_CONFIG = "hive.home";

    public static final String HIVE_HOME_DOC = "Hive home directory.";

    public static final String HIVE_HOME_DEFAULT = "";

    public static final String HIVE_HOME_DISPLAY = "Hive home directory";

    public static final String HIVE_DATABASE_CONFIG = "hive.database";

    public static final String HIVE_DATABASE_DOC =
        "The database to use when the connector creates tables in Hive.";

    public static final String HIVE_DATABASE_DEFAULT = "default";

    public static final String HIVE_DATABASE_DISPLAY = "Hive database";

    // CHECKSTYLE:OFF
    public static final ConfigDef.Recommender hiveIntegrationDependentsRecommender =
        new BooleanParentRecommender(HIVE_INTEGRATION_CONFIG);
    // CHECKSTYLE:ON

    protected static final ConfigDef CONFIG_DEF = new ConfigDef();

    static {
        // Define Hive configuration group.
        final String group = "Hive";
        int orderInGroup = 0;

        CONFIG_DEF.define(
            HIVE_INTEGRATION_CONFIG,
            Type.BOOLEAN,
            HIVE_INTEGRATION_DEFAULT,
            Importance.HIGH,
            HIVE_INTEGRATION_DOC,
            group,
            ++orderInGroup,
            Width.SHORT,
            HIVE_INTEGRATION_DISPLAY,
            Arrays.asList(
                HIVE_METASTORE_URIS_CONFIG,
                HIVE_CONF_DIR_CONFIG,
                HIVE_HOME_CONFIG,
                HIVE_DATABASE_CONFIG
            )
        );

        CONFIG_DEF.define(
            HIVE_METASTORE_URIS_CONFIG,
            Type.STRING,
            HIVE_METASTORE_URIS_DEFAULT,
            Importance.HIGH,
            HIVE_METASTORE_URIS_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            HIVE_METASTORE_URIS_DISPLAY,
            hiveIntegrationDependentsRecommender
        );

        CONFIG_DEF.define(
            HIVE_CONF_DIR_CONFIG,
            Type.STRING,
            HIVE_CONF_DIR_DEFAULT,
            Importance.HIGH,
            HIVE_CONF_DIR_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            HIVE_CONF_DIR_DISPLAY,
            hiveIntegrationDependentsRecommender
        );

        CONFIG_DEF.define(
            HIVE_HOME_CONFIG,
            Type.STRING,
            HIVE_HOME_DEFAULT,
            Importance.HIGH,
            HIVE_HOME_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            HIVE_HOME_DISPLAY,
            hiveIntegrationDependentsRecommender
        );

        CONFIG_DEF.define(
            HIVE_DATABASE_CONFIG,
            Type.STRING,
            HIVE_DATABASE_DEFAULT,
            Importance.HIGH,
            HIVE_DATABASE_DOC,
            group,
            ++orderInGroup,
            Width.SHORT,
            HIVE_DATABASE_DISPLAY,
            hiveIntegrationDependentsRecommender
        );
    }

    public HiveConfig(final Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    @Override
    public Object get(final String key) {
        return super.get(key);
    }

    /**
     * Get config definition.
     * @return configDef
     */
    public static ConfigDef getConfig() {
        return CONFIG_DEF;
    }

    public static class BooleanParentRecommender implements ConfigDef.Recommender {

        private final String parentConfigName;

        public BooleanParentRecommender(final String parentConfigName) {
            this.parentConfigName = parentConfigName;
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            return (boolean) connectorConfigs.get(parentConfigName);
        }
    }
}
