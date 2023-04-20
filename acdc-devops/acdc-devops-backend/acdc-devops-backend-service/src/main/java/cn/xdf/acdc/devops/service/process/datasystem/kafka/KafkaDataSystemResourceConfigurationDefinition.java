package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationValueType;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Arrays;

public class KafkaDataSystemResourceConfigurationDefinition {

    public static class Cluster {

        public static final ConfigurationDefinition<String> USERNAME = Authorization.USERNAME;

        public static final ConfigurationDefinition<String> PASSWORD = Authorization.PASSWORD;

        private static final String BOOTSTRAP_SERVERS_NAME = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

        private static final String BOOTSTRAP_SERVERS_DESC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

        public static final ConfigurationDefinition<String> BOOTSTRAP_SERVERS = new ConfigurationDefinition(
                false,
                false,
                BOOTSTRAP_SERVERS_NAME,
                BOOTSTRAP_SERVERS_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true
        );

        private static final String SASL_MECHANISM_NAME = SaslConfigs.SASL_MECHANISM;

        private static final String SASL_MECHANISM_DESC = SaslConfigs.SASL_MECHANISM_DOC;

        public static final ConfigurationDefinition<String> SASL_MECHANISM = new ConfigurationDefinition(
                true,
                false,
                SASL_MECHANISM_NAME,
                SASL_MECHANISM_DESC,
                SaslMechanism.SCRAM_SHA_512.getName(),
                ConfigurationValueType.ENUM,
                Arrays.stream(SaslMechanism.values()).map(SaslMechanism::getName).toArray(), value -> true
        );

        private static final String SECURITY_PROTOCOL_CONFIG_NAME = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

        private static final String SECURITY_PROTOCOL_CONFIG_DESC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;

        public static final ConfigurationDefinition<SecurityProtocol> SECURITY_PROTOCOL_CONFIG = new ConfigurationDefinition(
                false,
                false,
                SECURITY_PROTOCOL_CONFIG_NAME,
                SECURITY_PROTOCOL_CONFIG_DESC,
                SecurityProtocol.SASL_PLAINTEXT,
                ConfigurationValueType.ENUM,
                new SecurityProtocol[]{SecurityProtocol.PLAINTEXT, SecurityProtocol.SASL_PLAINTEXT}, value -> true
        );
    }
}
