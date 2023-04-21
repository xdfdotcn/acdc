package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class KafkaConfigurationUtilTest {

    private static final String SASL_JAAS_CONFIG_PATTERN = "%s required username=\"%s\" password=\"%s\";";

    @Test
    public void testGenerateAdminClientConfigurationShouldNotDecryptJaasConfigWhenSecurityProtocolIsSaslPlaintText() {
        DataSystemResourceDetailDTO kafkaClusterDetail = generateClusterDetail();

        Map<String, Object> adminClientConfiguration = KafkaConfigurationUtil.generateAdminClientConfiguration(kafkaClusterDetail);

        // assert
        Map<String, Object> desiredClientConfiguration = new HashMap<>();
        desiredClientConfiguration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.BOOTSTRAP_SERVERS.getName()).getValue());
        desiredClientConfiguration
                .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.SECURITY_PROTOCOL_CONFIG.getName()).getValue());
        desiredClientConfiguration.put(SaslConfigs.SASL_MECHANISM, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.SASL_MECHANISM.getName()).getValue());

        String username = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue();
        String encryptedPassword = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue();
        String decryptedPassword = EncryptUtil.decrypt(encryptedPassword);
        desiredClientConfiguration
                .put(SaslConfigs.SASL_JAAS_CONFIG, EncryptUtil.encrypt(String.format(SASL_JAAS_CONFIG_PATTERN, ScramLoginModule.class.getCanonicalName(), username, decryptedPassword)));
        Assertions.assertThat(adminClientConfiguration).isEqualTo(desiredClientConfiguration);
    }

    @Test
    public void testDecryptGenerateAdminClientConfigurationShouldDecryptJaasConfigWhenSecurityProtocolIsSaslPlaintText() {
        DataSystemResourceDetailDTO kafkaClusterDetail = generateClusterDetail();
        Map<String, Object> adminClientConfiguration = KafkaConfigurationUtil.generateDecryptAdminClientConfiguration(kafkaClusterDetail);

        // assert
        Map<String, Object> desiredClientConfiguration = new HashMap<>();
        desiredClientConfiguration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.BOOTSTRAP_SERVERS.getName()).getValue());
        desiredClientConfiguration
                .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.SECURITY_PROTOCOL_CONFIG.getName()).getValue());
        desiredClientConfiguration.put(SaslConfigs.SASL_MECHANISM, kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.SASL_MECHANISM.getName()).getValue());

        String username = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue();
        String encryptedPassword = kafkaClusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue();
        String decryptedPassword = EncryptUtil.decrypt(encryptedPassword);
        desiredClientConfiguration.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(SASL_JAAS_CONFIG_PATTERN, ScramLoginModule.class.getCanonicalName(), username, decryptedPassword));
        Assertions.assertThat(adminClientConfiguration).isEqualTo(desiredClientConfiguration);
    }

    private DataSystemResourceDetailDTO generateClusterDetail() {
        DataSystemResourceConfigurationDTO securityProtocol = DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.SECURITY_PROTOCOL_CONFIG.getName())
                .value("SASL_PLAINTEXT")
                .build();

        DataSystemResourceConfigurationDTO mechanism = DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.SASL_MECHANISM.getName())
                .value("SCRAM-SHA-512")
                .build();

        DataSystemResourceConfigurationDTO username = DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.USERNAME.getName())
                .value("user_name")
                .build();

        DataSystemResourceConfigurationDTO encryptedPassword = DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.PASSWORD.getName())
                .value(EncryptUtil.encrypt("password"))
                .build();

        DataSystemResourceConfigurationDTO bootstrapServers = DataSystemResourceConfigurationDTO.builder()
                .name(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                .value("6.6.6.2:6662")
                .build();

        Map<String, DataSystemResourceConfigurationDTO> configurations = new HashMap<>();
        configurations.put(securityProtocol.getName(), securityProtocol);
        configurations.put(mechanism.getName(), mechanism);
        configurations.put(username.getName(), username);
        configurations.put(encryptedPassword.getName(), encryptedPassword);
        configurations.put(bootstrapServers.getName(), bootstrapServers);

        DataSystemResourceDetailDTO kafkaClusterDetail = new DataSystemResourceDetailDTO();
        kafkaClusterDetail.setDataSystemResourceConfigurations(configurations);

        return kafkaClusterDetail;
    }

    @Test
    public void testGenerateAdminClientConfigurationShouldNotContainsSaslConfigurationWhenSecurityProtocolIsPlaintText() {
        DataSystemResourceConfigurationDTO securityProtocol = DataSystemResourceConfigurationDTO.builder()
                .name(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
                .value("PLAINTEXT")
                .build();

        DataSystemResourceConfigurationDTO bootstrapServers = DataSystemResourceConfigurationDTO.builder()
                .name("bootstrap.servers")
                .value("6.6.6.2:6662")
                .build();

        Map<String, DataSystemResourceConfigurationDTO> configurations = new HashMap<>();
        configurations.put(securityProtocol.getName(), securityProtocol);
        configurations.put(bootstrapServers.getName(), bootstrapServers);

        DataSystemResourceDetailDTO kafkaClusterDetail = new DataSystemResourceDetailDTO();
        kafkaClusterDetail.setDataSystemResourceConfigurations(configurations);

        Map<String, Object> adminClientConfiguration = KafkaConfigurationUtil.generateAdminClientConfiguration(kafkaClusterDetail);

        Assertions.assertThat(adminClientConfiguration.get(securityProtocol.getName())).isEqualTo(securityProtocol.getValue());
        Assertions.assertThat(adminClientConfiguration.get(bootstrapServers.getName())).isEqualTo(bootstrapServers.getValue());
        Assertions.assertThat(adminClientConfiguration).doesNotContainKey(SaslConfigs.SASL_MECHANISM);
        Assertions.assertThat(adminClientConfiguration).doesNotContainKey(SaslConfigs.SASL_JAAS_CONFIG);
    }
}
