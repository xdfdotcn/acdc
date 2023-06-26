package cn.xdf.acdc.devops.service.process.datasystem.es;

import java.util.Set;

import com.google.common.collect.Sets;

public class EsDataSystemConstant {

    public static class Connector {

        public static class Sink {

            public static class Configuration {

                public static final Set<String> SENSITIVE_CONFIGURATION_NAMES = Sets.newHashSet("connection.password");

                public static final String TOPICS = "topics";

                public static final String CONNECTION_URL = "connection.url";

                public static final String CONNECTION_USERNAME = "connection.username";

                public static final String CONNECTION_PASSWORD = "connection.password";

                public static final String TRANSFORMS = "transforms";

                public static final String TRANSFORMS_REPLACEFIELD = "transforms.replaceField";

                public static final String TRANSFORMS_REPLACEFIELD_WHITELIST = "transforms.replaceField.whitelist";

                public static final String TRANSFORMS_REPLACEFIELD_RENAME = "transforms.replaceField.renames";

                public static final String TRANSFORMS_TOPIC_TO_INDEX_REPLACEMENT = "transforms.topicToIndex.replacement";

            }
        }
    }
}
