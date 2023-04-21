package cn.xdf.acdc.devops.service.process.kafka.impl;

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.SaslConfigs;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional

public class KafkaClusterServiceImplTest {

    private static final String KAFKA_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"test\" password=\"123\";";

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
    }

    // TODO, used by ctl
    @Test
    public void testCreate() {
    }

    @Test
    public void testGetById() {
        KafkaClusterDO kafkaClusterDO = saveACDCKafkaCluster();
        KafkaClusterDTO kafkaClusterDTO = kafkaClusterService.getById(kafkaClusterDO.getId());
        Assertions.assertThat(kafkaClusterDTO.getId()).isEqualTo(kafkaClusterDO.getId());
    }

    @Test
    public void testGetByBootstrapServers() {
        KafkaClusterDO kafkaClusterDO = saveACDCKafkaCluster();
        Assertions.assertThat(kafkaClusterService.getByBootstrapServers(kafkaClusterDO.getBootstrapServers())
                .isPresent()).isTrue();
    }

    @Test
    public void testGetACDCKafkaCluster() {
        saveACDCKafkaCluster();
        Assertions.assertThat(kafkaClusterService.getACDCKafkaCluster()).isNotNull();
    }

    @Test
    public void testGetTICDCKafkaCluster() {
        saveTICDCKafkaCluster();
        Assertions.assertThat(kafkaClusterService.getTICDCKafkaCluster()).isNotNull();
    }

    @Test
    public void testGetDecryptedAdminConfig() {
        KafkaClusterDO savedKafkaCluster = saveACDCKafkaCluster();
        Map<String, Object> securityConfig = kafkaClusterService.getDecryptedAdminConfig(savedKafkaCluster.getId());
        Assertions.assertThat(securityConfig.get(SaslConfigs.SASL_JAAS_CONFIG)).isEqualTo(KAFKA_SASL_JAAS_CONFIG);
    }

    @Test
    public void testDeleteById() {
        KafkaClusterDO savedKafkaCluster = saveACDCKafkaCluster();
        kafkaClusterService.deleteById(savedKafkaCluster.getId());
        Assertions.assertThat(Assertions.catchThrowable(() -> kafkaClusterService.getById(savedKafkaCluster.getId())
        )).isInstanceOf(EntityNotFoundException.class);
    }

    private KafkaClusterDO saveACDCKafkaCluster() {
        Map<String, Object> securityConfig = new HashMap<>();
        securityConfig.put("security.protocol", "SASL_PLAINTEXT");
        securityConfig.put("sasl.mechanism", "SCRAM-SHA-512");
        securityConfig.put("sasl.jaas.config", EncryptUtil.encrypt(KAFKA_SASL_JAAS_CONFIG));

        try {
            objectMapper.writeValueAsString(securityConfig);
            return kafkaClusterRepository.save(KafkaClusterDO.builder()
                    .id(1L)
                    .name("test_kafka_cluster_ACDC")
                    .clusterType(KafkaClusterType.INNER)
                    .version("2.6")
                    .securityConfiguration(objectMapper.writeValueAsString(securityConfig))
                    .bootstrapServers("localhost:9092")
                    .build()
            );
        } catch (JsonProcessingException e) {
            throw new AcdcServiceException(e);
        }
    }

    private KafkaClusterDO saveTICDCKafkaCluster() {
        return kafkaClusterRepository.save(KafkaClusterDO.builder()
                .id(1L)
                .name("test_kafka_cluster_TICDC")
                .version("2.6")
                .securityConfiguration("{}")
                .clusterType(KafkaClusterType.TICDC)
                .bootstrapServers("localhost:9093")
                .build()
        );
    }
}
