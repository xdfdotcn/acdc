package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class AbstractSinkConnectorProcessServiceImplTest {

    private AbstractSinkConnectorProcessServiceImpl abstractSinkConnectProcessService;

    @Before
    public void setup() {
        abstractSinkConnectProcessService = new MysqlSinkConnectorProcessServiceImpl();
    }

    @Test
    public void testSetFilterExpression() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("destinations.test_tb.row.filter", "id<=1");

        Map<String, String> configMap = Maps.newHashMap();
        abstractSinkConnectProcessService.setFilterExpression("test_tb", "id<=1", configMap);

        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testSetConfFieldAdd() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("destinations.test_tb.fields.add", "hive_date:${datetime},my_date:${datetime}");

        Map<String, String> configMap = Maps.newHashMap();
        List<ConnectorDataExtensionDO> extensions = Lists.newArrayList(
                new ConnectorDataExtensionDO().setName("hive_date").setValue("${datetime}"),
                new ConnectorDataExtensionDO().setName("my_date").setValue("${datetime}")
        );
        abstractSinkConnectProcessService.setConfFieldAdd("test_tb", extensions, configMap);
        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testSetConfFieldMapping() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("destinations.test_tb.fields.mapping", "id:tid,name:tname,email:temail,__op:opt,__kafka_record_offset:version");

        List<SinkConnectorColumnMappingDO> mappings = Lists.newArrayList(
                SinkConnectorColumnMappingDO.builder().sourceColumnName("id\tbigint(20)\tPRI").sinkColumnName("tid\tbigint(20)\tPRI").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("name\tvarchar(20)").sinkColumnName("tname\tvarchar(20)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("email\tvarchar(20)").sinkColumnName("temail\tvarchar(20)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__logical_del\tstring").sinkColumnName("yn\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__datetime\tstring").sinkColumnName("ods_update_time\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__op\tstring").sinkColumnName("opt\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__kafka_record_offset\tstring").sinkColumnName("version\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__none\tstring").sinkColumnName("my_field\tvarchar(15)").build()
        );

        Map<String, String> configMap = Maps.newHashMap();

        abstractSinkConnectProcessService.setConfFieldMapping("test_tb", mappings, configMap);
        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testSetConfFieldWhitelist() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("destinations.test_tb.fields.whitelist", "id,name,email");

        Map<String, String> configMap = Maps.newHashMap();
        List<SinkConnectorColumnMappingDO> mappings = Lists.newArrayList(
                SinkConnectorColumnMappingDO.builder().sourceColumnName("id\tbigint(20)\tPRI").sinkColumnName("tid\tbigint(20)\tPRI").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("name\tvarchar(20)").sinkColumnName("tname\tvarchar(20)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("email\tvarchar(20)").sinkColumnName("temail\tvarchar(20)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__logical_del\tstring").sinkColumnName("yn\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__datetime\tstring").sinkColumnName("ods_update_time\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__op\tstring").sinkColumnName("opt\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__kafka_record_offset\tstring").sinkColumnName("version\tvarchar(15)").build(),
                SinkConnectorColumnMappingDO.builder().sourceColumnName("__none\tstring").sinkColumnName("my_field\tvarchar(15)").build()
        );

        abstractSinkConnectProcessService.setConfFieldWhitelist("test_tb", mappings, configMap);

        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testSetConfLogicalDelWithNoneModel() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("destinations.test_tb.delete.mode", "NONE");

        Map<String, String> configMap = Maps.newHashMap();
        abstractSinkConnectProcessService.setConfLogicalDel("test_tb", configMap);
        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }

    @Test
    public void testSetConfLogicalDel() {
        Map<String, String> expectConfigMap = Maps.newHashMap();
        expectConfigMap.put("destinations.test_tb.delete.mode", "LOGICAL");
        expectConfigMap.put("destinations.test_tb.delete.logical.field.name", "is_deleted");
        expectConfigMap.put("destinations.test_tb.delete.logical.field.value.deleted", "1");
        expectConfigMap.put("destinations.test_tb.delete.logical.field.value.normal", "0");

        Map<String, String> configMap = Maps.newHashMap();
        abstractSinkConnectProcessService.setConfLogicalDel("test_tb", "is_deleted", "1", "0", configMap);
        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }
}
