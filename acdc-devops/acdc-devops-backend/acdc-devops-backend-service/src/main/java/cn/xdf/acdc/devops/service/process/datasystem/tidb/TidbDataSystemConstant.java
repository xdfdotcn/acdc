package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class TidbDataSystemConstant {

    public static final String RESULT = "result";

    public static class Metadata {

        static class UserPermissionsAndBinlogConfiguration {

            public static final String RESULT = "result";

            public static final String ALL_PRIVILEGES = "ALL PRIVILEGES";

            public static final List<String[]> PERMISSIONS_FOR_UPDATE = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"INSERT"}, new String[]{"UPDATE"},
                    new String[]{"DELETE"});
        }

        public static class Tidb {

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

    static class Connector {

        static class Source {

            static class Ticdc {

                public static final String TICDC_TOPIC_NAME_PREFIX = "ticdc";
            }

            static class Configuration {

                public static final Set<String> IMMUTABLE_CONFIGURATION_NAMES = Sets.newHashSet(
                        Configuration.DATABASE_SERVER_NAME,
                        Configuration.SOURCE_KAFKA_TOPIC,
                        Configuration.SOURCE_KAFKA_GROUP_ID
                );

                public static final Set<String> SENSITIVE_CONFIGURATION_KEYS = Sets.newHashSet("source.kafka.sasl.jaas.config");

                public static final String CONNECTOR_NAME_PREFIX = "source-tidb";

                public static final String DATABASE_SERVER_NAME = "database.server.name";

                public static final String DATABASE_INCLUDE = "database.include";

                public static final String TABLE_INCLUDE_LIST = "table.include.list";

                public static final String MESSAGE_KEY_COLUMNS = "message.key.columns";

                public static final String SOURCE_KAFKA_TOPIC = "source.kafka.topic";

                public static final String SOURCE_KAFKA_GROUP_ID = "source.kafka.group.id";

                public static final String SOURCE_KAFKA_BOOTSTRAP_SERVERS = "source.kafka.bootstrap.servers";

                public static final String SOURCE_KAFKA_PREFIX = "source.kafka.";
            }
        }

        static class Sink {

            static class Configuration {

                public static final Set<String> SENSITIVE_CONFIGURATION_NAMES = Sets.newHashSet(Configuration.CONNECTION_PASSWORD);

                public static final String CONNECTOR_NAME_PREFIX = "sink-tidb";

                public static final String TOPICS = "topics";

                public static final String DESTINATIONS = "destinations";

                public static final String CONNECTION_URL = "connection.url";

                public static final String CONNECTION_PASSWORD = "connection.password";

                public static final String CONNECTION_USER = "connection.user";
            }
        }
    }
}
