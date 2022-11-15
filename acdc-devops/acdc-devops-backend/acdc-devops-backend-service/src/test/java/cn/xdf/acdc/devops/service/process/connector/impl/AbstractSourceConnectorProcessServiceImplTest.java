package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractSourceConnectorProcessServiceImplTest {

    private AbstractSourceConnectorProcessServiceImpl abstractSourceConnectProcessService;

    @Before
    public void init() {
        abstractSourceConnectProcessService = new MysqlSourceConnectorProcessServiceImpl();
    }

    @Test
    public void testPatchConfig() {
        Map<String, String> configMap = Maps.newHashMap();
        configMap.put("name", "source-mysql-rdb1-test_db");
        configMap.put("database.hostname", "localhost");
        configMap.put("database.port", "3306");
        configMap.put("database.user", "acdc");
        configMap.put("database.password", "acdc");
        configMap.put("database.server.name", "source-mysql-rdb1-test_db");
        configMap.put("database.include", "test_db");
        configMap.put("table.include.list", "test_db.test_tb");
        configMap.put("message.key.columns", "test_db.test_tb:id,uid");
        configMap.put("database.history.kafka.topic", "schema_history-source-mysql-rdb1-test_db");

        List<ConnectorConfigurationDO> configurations = createConfigurations(configMap);
        Map<String, String> patchConfigMap = abstractSourceConnectProcessService
            .patchConfig(configurations, "test_db", "test_tb2", Lists.newArrayList("id", "tid"));

        Assertions.assertThat("test_db.test_tb,test_db.test_tb2").isEqualTo(patchConfigMap.get("table.include.list"));
        Assertions.assertThat("test_db.test_tb:id,uid;test_db.test_tb2:id,tid")
            .isEqualTo(patchConfigMap.get("message.key.columns"));

        configMap.remove("table.include.list");
        configMap.remove("message.key.columns");
        patchConfigMap.remove("table.include.list");
        patchConfigMap.remove("message.key.columns");

        Assertions.assertThat(configMap).containsAllEntriesOf(patchConfigMap);
    }

    private List<ConnectorConfigurationDO> createConfigurations(final Map<String, String> configMap) {
        return configMap.entrySet().stream()
            .map(it -> {
                ConnectorConfigurationDO conf = new ConnectorConfigurationDO();
                conf.setName(it.getKey());
                conf.setValue(it.getValue());
                return conf;
            }).collect(Collectors.toList());
    }
}
