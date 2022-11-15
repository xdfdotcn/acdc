package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class MysqlSourceConnectorProcessServiceImplTest extends ConnectBaseTest {

    @Mock
    private RdbInstanceService rdbInstanceService;

    private MysqlSourceConnectorProcessServiceImpl mysqlSourceConnectProcessService;

    @Before
    public void setup() {
        ObjectMapper objectMapper = new ObjectMapper();
        mysqlSourceConnectProcessService = new MysqlSourceConnectorProcessServiceImpl();
        mysqlSourceConnectProcessService.rdbJdbcConfig = rdbJdbcConfig;
        ReflectionTestUtils.setField(mysqlSourceConnectProcessService, "rdbInstanceService", rdbInstanceService);
        ReflectionTestUtils.setField(mysqlSourceConnectProcessService, "objectMapper", objectMapper);
    }

    @Test(expected = NotFoundException.class)
    public void testCheckOrInitDataSource() {
        mysqlSourceConnectProcessService.checkOrInitDataSource(new RdbDO(), new RdbDatabaseDO());
    }

    @Test
    public void testFetchConfig() {
        String encryptSASLConfig = EncryptUtil.encrypt(ConnectBaseTest.INNER_SASL_JAAS_CONFIG);
        String encryptDatabasePassWord = EncryptUtil.encrypt("acdc");

        Map<String, String> configMap = mysqlSourceConnectProcessService.fetchConfig(
            rdb,
            rdbInstance,
            rdbDatabase,
            rdbTable,
            dataKafkaTopic,
            uniqueKeys);

        Map<String, String> expect = Maps.newHashMap();
        expect.put("name", "source-mysql-rdb1-test_db");
        expect.put("database.hostname", "localhost");
        expect.put("database.port", "3306");
        expect.put("database.user", "acdc");
        expect.put("database.password", encryptDatabasePassWord);
        expect.put("database.server.name", "source-mysql-rdb1-test_db");
        expect.put("database.include", "test_db");
        expect.put("table.include.list", "test_db.test_tb");
        expect.put("message.key.columns", "test_db.test_tb:id,uid");
        expect.put("database.history.kafka.topic", "schema_history-source-mysql-rdb1-test_db");
        expect.put("database.history.consumer.sasl.jaas.config", encryptSASLConfig);
        expect.put("database.history.producer.sasl.jaas.config", encryptSASLConfig);
        expect.put("database.history.kafka.bootstrap.servers", "localhost:9093");
        Assertions.assertThat(expect).containsAllEntriesOf(configMap);
    }

    @Test
    public void testGetDecryptConfig() {
        Assertions.assertThat(MysqlSourceConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET.size()).isEqualTo(3);
        Assertions.assertThat(MysqlSourceConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET).contains(
            "database.history.producer.sasl.jaas.config",
            "database.history.consumer.sasl.jaas.config",
            "database.password"
        );
    }
}
