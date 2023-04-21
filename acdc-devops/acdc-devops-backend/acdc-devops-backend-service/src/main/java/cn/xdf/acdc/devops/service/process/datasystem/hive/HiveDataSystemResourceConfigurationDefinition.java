package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationValueType;

public class HiveDataSystemResourceConfigurationDefinition {

    public static class Hdfs {

        private static final String HDFS_NAME_SERVICES_KEY = "hdfs.name.services";

        private static final String HDFS_NAME_SERVICES_DESC = "hdfs name services, eg: testCluster";

        private static final String HDFS_NAME_NODES_KEY = "hdfs.ha.name.nodes";

        private static final String HDFS_NAME_NODES_DESC = "hdfs high availability name nodes, eg: nn1=name-node-01:8020,nn2=name-node-02:8020";

        private static final String HDFS_HADOOP_USER_KEY = "hdfs.hadoop.user";

        private static final String HDFS_HADOOP_USER_DESC = "the hadoop user, eg: hive";

        private static final String HDFS_HADOOP_USER_DEFAULT_VALUE = "hive";

        private static final String HDFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY = "hdfs.client.failover.proxy.provider";

        private static final String HDFS_CLIENT_FAILOVER_PROXY_PROVIDER_DESC = "hdfs client failover proxy provider class name, "
                + "eg: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";

        public static final ConfigurationDefinition<String> HDFS_NAME_SERVICES = new ConfigurationDefinition(
                false,
                false,
                HDFS_NAME_SERVICES_KEY,
                HDFS_NAME_SERVICES_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);

        public static final ConfigurationDefinition<String> HDFS_NAME_NODES = new ConfigurationDefinition(
                false,
                false,
                HDFS_NAME_NODES_KEY,
                HDFS_NAME_NODES_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);

        public static final ConfigurationDefinition<String> HDFS_HADOOP_USER = new ConfigurationDefinition(
                false,
                false,
                HDFS_HADOOP_USER_KEY,
                HDFS_HADOOP_USER_DESC,
                HDFS_HADOOP_USER_DEFAULT_VALUE,
                ConfigurationValueType.STRING,
                new String[0], value -> true);

        public static final ConfigurationDefinition<String> HDFS_CLIENT_FAILOVER_PROXY_PROVIDER = new ConfigurationDefinition(
                false,
                false,
                HDFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY,
                HDFS_CLIENT_FAILOVER_PROXY_PROVIDER_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);

    }

    public static class Hive {

        private static final String HIVE_METASTORE_URIS_KEY = "hive.metastore.uris";

        private static final String HIVE_METASTORE_URIS_DESC = "hive metastore url";

        public static final ConfigurationDefinition<String> HIVE_METASTORE_URIS = new ConfigurationDefinition(
                false,
                false,
                HIVE_METASTORE_URIS_KEY,
                HIVE_METASTORE_URIS_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);
    }
}
