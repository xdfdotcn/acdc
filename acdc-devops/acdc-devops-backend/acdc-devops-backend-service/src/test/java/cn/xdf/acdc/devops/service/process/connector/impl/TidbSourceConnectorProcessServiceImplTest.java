package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseTidbService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TidbSourceConnectorProcessServiceImplTest extends ConnectBaseTest {

    @Mock
    private KafkaClusterService kafkaClusterService;

    @Mock
    private KafkaTopicService kafkaTopicService;

    @Mock
    private RdbDatabaseTidbService rdbDatabaseTidbService;

    private TidbSourceConnectorProcessServiceImpl tidbSourceConnectProcessService;

    @Before
    public void setup() {
        ObjectMapper objectMapper = new ObjectMapper();
        tidbSourceConnectProcessService = new TidbSourceConnectorProcessServiceImpl();
        tidbSourceConnectProcessService.rdbJdbcConfig = rdbJdbcConfig;
        ReflectionTestUtils.setField(tidbSourceConnectProcessService, "kafkaClusterService", kafkaClusterService);
        ReflectionTestUtils.setField(tidbSourceConnectProcessService, "kafkaTopicService", kafkaTopicService);
        ReflectionTestUtils.setField(tidbSourceConnectProcessService, "rdbDatabaseTidbService", rdbDatabaseTidbService);
        ReflectionTestUtils.setField(tidbSourceConnectProcessService, "objectMapper", objectMapper);
    }

    @Test
    public void testCheckOrInitDataSource() {
        when(kafkaClusterService.findTicdcKafkaCluster()).thenReturn(Optional.of(new KafkaClusterDO()));
        tidbSourceConnectProcessService.checkOrInitDataSource(RdbDO.builder().name("rdb").build(), RdbDatabaseDO.builder().name("db").build());

        Mockito.verify(kafkaTopicService, Mockito.times(1)).save(any());
        Mockito.verify(rdbDatabaseTidbService, Mockito.times(1)).save(any());
    }

    @Test
    public void testFetchConfig() {
        String encryptSASLConfig = EncryptUtil.encrypt(ConnectBaseTest.TICDC_SASL_JAAS_CONFIG);
        when(kafkaClusterService.findTicdcKafkaCluster())
            .thenReturn(Optional.of(ticdcKafkaCluster));
        Map<String, String> configMap = tidbSourceConnectProcessService.fetchConfig(
            rdb,
            rdbInstance,
            rdbDatabase,
            rdbTable,
            dataKafkaTopic,
            uniqueKeys);

        Map<String, String> expect = Maps.newHashMap();
        expect.put("name", "source-tidb-rdb1-test_db");
        expect.put("database.hostname", "localhost");
        expect.put("database.password", "acdc");
        expect.put("database.server.name", "source-tidb-rdb1-test_db");
        expect.put("database.include", "test_db");
        expect.put("table.include.list", "test_db.test_tb");
        expect.put("message.key.columns", "test_db.test_tb:id,uid");
        expect.put("source.kafka.topic", "tidb-rdb1-test_db");
        expect.put("source.kafka.group.id", "source-tidb-rdb1-test_db");
        expect.put("source.kafka.sasl.jaas.config", encryptSASLConfig);
        expect.put("source.kafka.bootstrap.servers", null);

        Assertions.assertThat(expect).containsAllEntriesOf(configMap);
    }

    @Test
    public void testGetDecryptConfig() {
        Assertions.assertThat(TidbSourceConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET.size()).isEqualTo(1);
        Assertions.assertThat(TidbSourceConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET).contains(
            "source.kafka.sasl.jaas.config"
        );
    }
}
