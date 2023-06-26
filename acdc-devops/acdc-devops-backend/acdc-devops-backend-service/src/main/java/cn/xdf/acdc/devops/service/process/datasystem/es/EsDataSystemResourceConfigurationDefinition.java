package cn.xdf.acdc.devops.service.process.datasystem.es;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationValueType;

public class EsDataSystemResourceConfigurationDefinition {

    public static class Cluster {

        public static final ConfigurationDefinition<String> USERNAME = Authorization.USERNAME;

        public static final ConfigurationDefinition<String> PASSWORD = Authorization.PASSWORD;

        private static final String NODE_SERVERS_KEY = "node.servers";

        private static final String NODE_SERVERS_DESC = "node servers, eg: server1:9200,server2:9200";

        public static final ConfigurationDefinition<String> NODE_SERVERS = new ConfigurationDefinition<>(
                false,
                false,
                NODE_SERVERS_KEY,
                NODE_SERVERS_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true
        );
    }
}
