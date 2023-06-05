package cn.xdf.acdc.devops.service.process.datasystem.hive;

public class HiveDataSystemConstant {
    
    public static class Metadata {
    
    }
    
    public static class Connector {
        
        public static class Sink {
            
            public static class Configuration {
                
                public static final String CONNECTOR_NAME_PREFIX = "sink-hive";
                
                public static final String TOPICS = "topics";
                
                public static final String DESTINATIONS = "destinations";
                
                public static final String HADOOP_USER = "hadoop.user";
                
                public static final String HDFS_URL_PROTOCOL = "hdfs://";
                
                public static final String STORE_URL = "store.url";
                
                public static final String HDFS_NAME_SERVICES = "__hdfs.dfs.nameservices";
                
                public static final String HDFS_HA_NAMENODES = "__hdfs.dfs.ha.namenodes";
                
                public static final String HDFS_NAME_NODE_RPC = "__hdfs.dfs.namenode.rpc-address";
                
                public static final String HDFS_CLIENT_FAILOVER_PROXY_PROVIDER = "__hdfs.dfs.client.failover.proxy.provider";
                
                public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
            }
        }
    }
}
