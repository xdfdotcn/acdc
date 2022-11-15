package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.entity.ConnectionColumnConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectionService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorColumnMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {FieldMappingProcessServiceManagerTest.Config.class})
public class FieldMappingProcessServiceManagerTest {

    @MockBean
    private ConnectorService connectorService;

    @MockBean
    private SinkConnectorService sinkConnectorService;

    @MockBean
    private SinkConnectorColumnMappingService sinkConnectorColumnMappingService;

    @MockBean
    private ConnectionService connectionService;

    @MockBean
    private ConnectionColumnConfigurationService connectionColumnConfigurationService;

    @MockBean
    private FieldService fieldService;

    @Autowired
    private FieldMappingProcessServiceManager fieldMappingProcessServiceManager;

    @Before
    public void setup() {
        Mockito.when(fieldService.supportAppTypes()).thenReturn(Sets.newSet(DataSystemType.MYSQL));
        fieldMappingProcessServiceManager.init(Lists.newArrayList(fieldService), Lists.newArrayList(new Jdbc2JdbcFieldMappingServiceImpl()));
    }

    @Test
    public void testGetConnectionColumnConfigurationsMergedWithCurrentDdlShouldUseCurrentDdlField() {
        Mockito.when(connectionService.findById(1L)).thenReturn(Optional.of(fakeConnection()));

        DataSetDTO sourceDataSet = DataSetDTO.builder().dataSystemType(DataSystemType.MYSQL).dataSetId(100L).build();
        Mockito.when(fieldService.descDataSet(ArgumentMatchers.eq(sourceDataSet))).thenReturn(fakeNewSourceField());

        DataSetDTO sinkDataSet = DataSetDTO.builder().dataSystemType(DataSystemType.MYSQL).dataSetId(200L).build();
        Mockito.when(fieldService.descDataSet(ArgumentMatchers.eq(sinkDataSet))).thenReturn(fakeNewSinkField());
        Mockito.when(connectionColumnConfigurationService.query(ArgumentMatchers.any())).thenReturn(fakeExistConfigFieldMapping());
        List<FieldMappingDTO> result =
                fieldMappingProcessServiceManager.getConnectionColumnConfigurationsMergedWithCurrentDdl(1L);

        //case1，主键id，遵循最新表结构，主键为id，且类型为bigint
        Assert.assertEquals("id", result.get(0).getSinkField().getName());
        Assert.assertEquals("bigint", result.get(0).getSinkField().getDataType());
        Assert.assertEquals("PRI", result.get(0).getSinkField().getKeyType());
        Assert.assertEquals("id", result.get(0).getSourceField().getName());
        Assert.assertEquals("bigint", result.get(0).getSourceField().getDataType());
        Assert.assertEquals("PRI", result.get(0).getSourceField().getKeyType());

        //case2，普通字段name且之前配置了字段映射，使用最新表结构和之前配置的映射关系
        Assert.assertEquals("name", result.get(1).getSinkField().getName());
        Assert.assertEquals("varchar", result.get(1).getSinkField().getDataType());
        Assert.assertNull(result.get(1).getSinkField().getKeyType());
        Assert.assertEquals("name", result.get(1).getSourceField().getName());
        Assert.assertEquals("varchar", result.get(1).getSourceField().getDataType());
        Assert.assertNull(result.get(1).getSourceField().getKeyType());

        //case3，address字段，之前不存在 但 能按字段名称匹配到，则遵循之前不存在时配置
        Assert.assertEquals("address", result.get(2).getSinkField().getName());
        Assert.assertEquals("", result.get(2).getSourceField().getName());

        //case4，gender字段之前配置了映射关系，但source该字段已经不存在，则移除映射关系
        Assert.assertEquals("gender", result.get(3).getSinkField().getName());
        Assert.assertEquals("", result.get(3).getSourceField().getName());

        //case5，iphone字段之前没有配置对应source字段，但按名称可以匹配到，则按原先配置处理
        Assert.assertEquals("iphone", result.get(4).getSinkField().getName());
        Assert.assertEquals("", result.get(4).getSourceField().getName());
    }

    private List<ConnectionColumnConfigurationDO> fakeExistConfigFieldMapping() {
        List<ConnectionColumnConfigurationDO> result = new ArrayList<>();
        result.add(ConnectionColumnConfigurationDO.builder().id(1L).sinkColumnName("id\tint").sourceColumnName("id\tint").build());
        result.add(ConnectionColumnConfigurationDO.builder().id(2L).sinkColumnName("name\ttext\tPRI").sourceColumnName("name\ttext").build());
        result.add(ConnectionColumnConfigurationDO.builder().id(3L).sinkColumnName("gender\tvarchar").sourceColumnName("gender\tvarchar").build());
        result.add(ConnectionColumnConfigurationDO.builder().id(4L).sinkColumnName("iphone\tvarchar").sourceColumnName("__none\tvarchar").build());
        return result;
    }

    private Map<String, FieldDTO> fakeNewSinkField() {
        Map<String, FieldDTO> result = new HashMap<>();
        result.put("id", FieldDTO.builder().name("id").dataType("bigint").keyType(FieldKeyType.PRI.name()).build());
        result.put("name", FieldDTO.builder().name("name").dataType("varchar").build());
        result.put("address", FieldDTO.builder().name("address").dataType("varchar").build());
        result.put("gender", FieldDTO.builder().name("gender").dataType("varchar").build());
        result.put("iphone", FieldDTO.builder().name("iphone").dataType("varchar").build());
        return result;
    }

    private Map<String, FieldDTO> fakeNewSourceField() {
        Map<String, FieldDTO> result = new HashMap<>();
        result.put("id", FieldDTO.builder().name("id").dataType("bigint").keyType(FieldKeyType.PRI.name()).build());
        result.put("name", FieldDTO.builder().name("name").dataType("varchar").build());
        result.put("address", FieldDTO.builder().name("address").dataType("varchar").build());
        result.put("iphone", FieldDTO.builder().name("iphone").dataType("varchar").build());
        return result;
    }

    private ConnectionDO fakeConnection() {
        ConnectionDO connectionDO = new ConnectionDO();
        connectionDO.setId(1L);
        connectionDO.setSourceDataSystemType(DataSystemType.MYSQL);
        connectionDO.setSourceDataSetId(100L);
        connectionDO.setSinkDataSystemType(DataSystemType.MYSQL);
        connectionDO.setSinkDataSetId(200L);
        return connectionDO;
    }

    @Configuration
    @ComponentScan("NeverBeIncluded")
    static class Config {

        @Bean
        public FieldMappingProcessServiceManager fieldMappingProcessServiceManager(final List<FieldService> fieldServices) {
            return new FieldMappingProcessServiceManager(fieldServices, new ArrayList<>());
        }
    }
}
