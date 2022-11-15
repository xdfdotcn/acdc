package cn.xdf.acdc.devops.service.process.connector.impl;

import com.google.common.collect.Maps;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractJdbcSinkConnectorProcessServiceImplTest extends ConnectBaseTest {

    @Test
    public void testMysqlFetchConfig() {
        AbstractJdbcSinkConnectorProcessServiceImpl mysqlSinkConnectProcessServiceImpl = new MysqlSinkConnectorProcessServiceImpl();

        Map<String, String> expectConfigMap = createJdbcSinkExpectConfigMap();
        expectConfigMap.put("name", "sink-mysql-rdb1-test_db-test_tb");

        Map<String, String> configMap = mysqlSinkConnectProcessServiceImpl.fetchConfig(
            dataKafkaTopic,
            rdb,
            rdbInstance,
            rdbDatabase,
            rdbTable,
            "id>1",
            logicalDelDTO,
            extensions,
            fieldMappings
        );

        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testMysqlGetDecryptConfig() {
        Assertions.assertThat(MysqlSinkConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET)
            .contains(
                "connection.password"
            );
    }

    @Test
    public void testTidbGetDecryptConfig() {
        Assertions.assertThat(TidbSinkConnectorProcessServiceImpl.ENCRYPT_CONF_ITEM_SET)
            .contains(
                "connection.password"
            );
    }

    @Test
    public void testTidbFetchConfig() {
        AbstractJdbcSinkConnectorProcessServiceImpl tidbSinkConnectProcessServiceImpl = new TidbSinkConnectorProcessServiceImpl();

        Map<String, String> expectConfigMap = createJdbcSinkExpectConfigMap();
        expectConfigMap.put("name", "sink-tidb-rdb1-test_db-test_tb");

        Map<String, String> configMap = tidbSinkConnectProcessServiceImpl.fetchConfig(
            dataKafkaTopic,
            rdb,
            rdbInstance,
            rdbDatabase,
            rdbTable,
            "id>1",
            logicalDelDTO,
            extensions,
            fieldMappings
        );

        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    private Map<String, String> createJdbcSinkExpectConfigMap() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("topics", "test_topic");
        expectConfigMap.put("connection.url", "jdbc:mysql://localhost:3306/test_db");
        expectConfigMap.put("destinations", "test_tb");
        expectConfigMap.put("connection.user", "acdc");
        expectConfigMap.put("connection.password", "dwhlNrioAjDU6weeClr42Q==");

        // filter
        expectConfigMap.put("destinations.test_tb.row.filter", "id>1");

        // add
        expectConfigMap.put("destinations.test_tb.fields.add", "test1_date:${datetime},test2_date:${datetime}");

        // mapping
        expectConfigMap.put("destinations.test_tb.fields.mapping",
            "id:tid,name:tname,email:temail,__op:opt,__kafka_record_offset:version");

        // whitelist
        expectConfigMap.put("destinations.test_tb.fields.whitelist", "id,name,email");

        // logical del
        expectConfigMap.put("destinations.test_tb.delete.mode", "LOGICAL");
        expectConfigMap.put("destinations.test_tb.delete.logical.field.name", "is_deleted");
        expectConfigMap.put("destinations.test_tb.delete.logical.field.value.deleted", "1");
        expectConfigMap.put("destinations.test_tb.delete.logical.field.value.normal", "0");
        return expectConfigMap;
    }
}
