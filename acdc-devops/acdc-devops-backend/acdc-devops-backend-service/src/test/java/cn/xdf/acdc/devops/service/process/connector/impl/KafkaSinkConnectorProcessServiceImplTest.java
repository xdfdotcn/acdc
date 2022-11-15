package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkConnectorProcessServiceImplTest extends ConnectBaseTest {

    private KafkaSinkConnectorProcessServiceImpl kafkaSinkConnectProcessService;

    @Before
    public void setup() {
        kafkaSinkConnectProcessService = new KafkaSinkConnectorProcessServiceImpl();
        ReflectionTestUtils.setField(kafkaSinkConnectProcessService, "objectMapper", new ObjectMapper());
    }

    @Test
    public void testFetchConfig() {
        fieldMappings = Lists.newArrayList(
            SinkConnectorColumnMappingDO.builder().sourceColumnName("id\tbigint(20)\tPRI")
                .sinkColumnName("id\tbigint(20)\tPRI").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("name\tvarchar(20)")
                .sinkColumnName("name\tvarchar(20)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("email\tvarchar(20)")
                .sinkColumnName("email\tvarchar(20)").build()
        );

        String encryptSASLConfig = EncryptUtil.encrypt(ConnectBaseTest.USER_SASL_JAAS_CONFIG);

        Map<String, String> expectConfigMap = Maps.newHashMap();

        //base
        expectConfigMap.put("name", "sink-kafka-test-mysql_test_db_test_tb_json");
        expectConfigMap.put("topics", "test_topic");
        expectConfigMap.put("destinations", "mysql_test_db_test_tb_json");

        // filter
        expectConfigMap.put("destinations.mysql_test_db_test_tb_json.row.filter", "id>1");

        // mapping
        expectConfigMap.put("destinations.mysql_test_db_test_tb_json.fields.mapping", "id:id,name:name,email:email");

        // whitelist
        expectConfigMap.put("destinations.mysql_test_db_test_tb_json.whitelist", "id,name,email");

        // logical del
        expectConfigMap.put("destinations.mysql_test_db_test_tb_json.delete.mode", "NONE");

        // kafka
        expectConfigMap.put("sink.kafka.bootstrap.servers", "localhost:9003");
        expectConfigMap.put("sink.kafka.sasl.jaas.config", encryptSASLConfig);
        expectConfigMap.put("sink.kafka.security.protocol", "SASL_PLAINTEXT");
        expectConfigMap.put("sink.kafka.sasl.mechanism", "SCRAM-SHA-512");
        expectConfigMap.put("sink.kafka.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        expectConfigMap.put("sink.kafka.value.converter", "org.apache.kafka.connect.json.JsonConverter");

        Map<String, String> configMap = kafkaSinkConnectProcessService.fetchConfig(
            userKafkaCluster,
            kafkaConverterType,
            dataKafkaTopic,
            sinkKafkaTopic,
            "id>1",
            fieldMappings);
        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testGetDecryptConfig() {
        Assertions.assertThat(KafkaSinkConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET.size()).isEqualTo(1);
        Assertions.assertThat(KafkaSinkConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET).contains(
            "sink.kafka.sasl.jaas.config"
        );
    }
}
