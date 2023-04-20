package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;

import java.util.HashMap;
import java.util.Map;

public class KafkaConfigurationUtil {

    private static final String SASL_JAAS_CONFIG_PATTERN = "%s required username=\"%s\" password=\"%s\";";

    /**
     * Generate kafka admin client configuration.
     *
     * <p>Cautious: jaas config in result will not decrypt.
     *
     * @param kafkaClusterDetail kafka cluster detail DTO
     * @return kafka admin configuration
     */
    public static Map<String, Object> generateAdminClientConfiguration(final DataSystemResourceDetailDTO kafkaClusterDetail) {
        Map<String, Object> configuration = new HashMap<>();
        String securityProtocol = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.SECURITY_PROTOCOL_CONFIG.getName()).getValue();
        configuration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        if (SecurityProtocol.SASL_PLAINTEXT.name.equals(securityProtocol)) {
            String saslMechanism = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.SASL_MECHANISM.getName()).getValue();

            // jaas configuration
            String username = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue();
            String encryptedPassword = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue();
            String decryptedPassword = EncryptUtil.decrypt(encryptedPassword);
            String jaasConfig;

            if (SaslMechanism.PLAIN.getName().equals(saslMechanism)) {
                jaasConfig = String.format(SASL_JAAS_CONFIG_PATTERN, PlainLoginModule.class.getCanonicalName(), username, decryptedPassword);
            } else if (SaslMechanism.SCRAM_SHA_256.getName().equals(saslMechanism) || SaslMechanism.SCRAM_SHA_512.getName().equals(saslMechanism)) {
                jaasConfig = String.format(SASL_JAAS_CONFIG_PATTERN, ScramLoginModule.class.getCanonicalName(), username, decryptedPassword);
            } else {
                throw new IllegalArgumentException(String.format("unsupported sasl mechanism %s", saslMechanism));
            }

            configuration.put(SaslConfigs.SASL_JAAS_CONFIG, EncryptUtil.encrypt(jaasConfig));
            configuration.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }
        configuration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.BOOTSTRAP_SERVERS.getName()).getValue());
        return configuration;
    }

    /**
     * Generate kafka admin client configuration.
     *
     * @param kafkaClusterDetail kafka cluster detail DTO
     * @return kafka admin configuration
     */
    public static Map<String, Object> generateDecryptAdminClientConfiguration(final DataSystemResourceDetailDTO kafkaClusterDetail) {
        Map<String, Object> configuration = generateAdminClientConfiguration(kafkaClusterDetail);
        configuration.computeIfPresent(SaslConfigs.SASL_JAAS_CONFIG, (key, value) -> EncryptUtil.decrypt(value.toString()));
        return configuration;
    }
}
