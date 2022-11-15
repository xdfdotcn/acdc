package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class KafkaClusterEncryptTest {

    public static final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule "
        + "required username=\\\"Admin\\\" password=\\\"666\\\";";

    public static final Map<String, String> KAFKA_CLUSTER_ADMIN_CONFIG = new HashMap<>();

    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void init() {
        KAFKA_CLUSTER_ADMIN_CONFIG.put("security.protocol", "SASL_PLAINTEXT");
        KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.mechanism", "SCRAM-SHA-512");
        KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.jaas.config", EncryptUtil.encrypt(SASL_JAAS_CONFIG));
    }

    @Test
    public void testAdminConfigEncrypt() throws JsonProcessingException {
        String jsonString = objectMapper.writeValueAsString(KAFKA_CLUSTER_ADMIN_CONFIG);

        Map<String, String> adminConfig = objectMapper.readValue(jsonString, Map.class);
        String decryptSaslJaasConfig = EncryptUtil.decrypt(adminConfig.get("sasl.jaas.config"));

        Assertions.assertThat(adminConfig.get("security.protocol")).isEqualTo("SASL_PLAINTEXT");
        Assertions.assertThat(adminConfig.get("sasl.mechanism")).isEqualTo("SCRAM-SHA-512");
        Assertions.assertThat(decryptSaslJaasConfig).isEqualTo(SASL_JAAS_CONFIG);
    }
}
