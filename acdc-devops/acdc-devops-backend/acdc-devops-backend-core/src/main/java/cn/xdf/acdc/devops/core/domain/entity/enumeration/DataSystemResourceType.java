package cn.xdf.acdc.devops.core.domain.entity.enumeration;

public enum DataSystemResourceType {

    MYSQL_CLUSTER, MYSQL_INSTANCE, MYSQL_DATABASE, MYSQL_TABLE,

    TIDB_CLUSTER, TIDB_SERVER, TIDB_DATABASE, TIDB_TABLE,

    KAFKA_CLUSTER, KAFKA_TOPIC,

    HIVE, HIVE_DATABASE, HIVE_TABLE,

    SQLSERVER_CLUSTER, SQLSERVER_INSTANCE, SQLSERVER_DATABASE, SQLSERVER_TABLE,

    ORACLE_CLUSTER, ORACLE_INSTANCE, ORACLE_DATABASE, ORACLE_TABLE,

    ELASTICSEARCH_CLUSTER, ELASTICSEARCH_INDEX,
    
    STARROCKS_CLUSTER, STARROCKS_FRONTEND, STARROCKS_DATABASE, STARROCKS_TABLE
}
