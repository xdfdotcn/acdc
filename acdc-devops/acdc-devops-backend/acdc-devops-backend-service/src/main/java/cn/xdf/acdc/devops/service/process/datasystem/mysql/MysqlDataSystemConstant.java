package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class MysqlDataSystemConstant {
    
    public static class Metadata {
        
        static class UserPermissionsAndBinlogConfiguration {
            
            public static final String RESULT = "result";
            
            public static final String SHOW_VARIABLES_SQL = "show variables like '%s'";
            
            public static final String SQL_MODE = "sql_mode";
            
            public static final String EXPECTED_SQL_MODE_VALUE = "STRICT_TRANS_TABLES";
            
            public static final String[] TO_CHECK_BINLOG_CONFIGURATION = new String[]{"log_bin", "binlog_format", "binlog_row_image", "expire_logs_days"};
            
            public static final String[] EXPECTED_BINLOG_CONFIGURATION_VALUE_EXPRESSION = new String[]{"string.contains(result,'ON')",
                    "string.contains(result,'ROW')", "string.contains(result,'FULL')", "result>=4 || result==0"};
            
            public static final String[] EXPECTED_BINLOG_CONFIGURATION_VALUE = new String[]{"ON", "ROW", "FULL", "4"};
            
            public static final List<String[]> PERMISSIONS_FOR_MASTER = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"INSERT"}, new String[]{"UPDATE"},
                    new String[]{"DELETE"});
            
            public static final List<String[]> PERMISSIONS_FOR_DATASOURCE = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"REPLICATION SLAVE"},
                    new String[]{"REPLICATION CLIENT"}, new String[]{"RELOAD", "LOCK TABLES"});
            
            public static final String EXPRESSION_ON = "string.contains(result,'ON')";
            
            public static final String EXPRESSION_EXPECTED_VALUE_ON = "ON";
            
            public static final String VARIABLES_LOG_SLAVE_UPDATES = "log_slave_updates";
        }
        
        public static class Mysql {
            
            public static final String PK_INDEX_NAME = "PRIMARY";
            
            public static final Set<String> SYSTEM_DATABASES = Sets.newHashSet(
                    "information_schema",
                    "mysql",
                    "performance_schema",
                    "sys",
                    "sys_operator"
            );
        }
    }
    
    public static class Connector {
        
        public static class Source {
            
            public static class Topic {
                
                public static final String SCHEMA_CHANGE_TOPIC_PREFIX = "schema_history";
            }
            
            public static class Configuration {
                
                public static final Set<String> IMMUTABLE_CONFIGURATION_NAMES = Sets.newHashSet(
                        Configuration.DATABASE_SERVER_NAME,
                        Configuration.DATABASE_HISTORY_KAFKA_TOPIC
                );
                
                public static final Set<String> SENSITIVE_CONFIGURATION_NAMES = Sets.newHashSet(
                        Configuration.DATABASE_PASSWORD,
                        "database.history.producer.sasl.jaas.config",
                        "database.history.consumer.sasl.jaas.config"
                );
                
                public static final String CONNECTOR_NAME_PREFIX = "source-mysql";
                
                public static final String DATABASE_HOSTNAME = "database.hostname";
                
                public static final String DATABASE_PORT = "database.port";
                
                public static final String DATABASE_USER = "database.user";
                
                public static final String DATABASE_PASSWORD = "database.password";
                
                public static final String DATABASE_SERVER_NAME = "database.server.name";
                
                public static final String DATABASE_HISTORY_KAFKA_TOPIC = "database.history.kafka.topic";
                
                public static final String DATABASE_HISTORY_CONSUMER_PREFIX = "database.history.consumer";
                
                public static final String DATABASE_HISTORY_PRODUCER_PREFIX = "database.history.producer";
                
                public static final String DATABASE_HISTORY_KAFKA_BOOTSTRAP_SERVERS = "database.history.kafka.bootstrap.servers";
                
                public static final String DATABASE_INCLUDE = "database.include";
                
                public static final String TABLE_INCLUDE_LIST = "table.include.list";
                
                public static final String MESSAGE_KEY_COLUMNS = "message.key.columns";
            }
        }
        
        public static class Sink {
            
            public static class Configuration {
                
                public static final Set<String> SENSITIVE_CONFIGURATION_NAMES = Sets.newHashSet("connection.password");
                
                public static final String CONNECTOR_NAME_PREFIX = "sink-mysql";
                
                public static final String TOPICS = "topics";
                
                public static final String DESTINATIONS = "destinations";
                
                public static final String CONNECTION_URL = "connection.url";
                
                public static final String CONNECTION_PASSWORD = "connection.password";
                
                public static final String CONNECTION_USER = "connection.user";
            }
        }
    }
}
