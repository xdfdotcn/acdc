package cn.xdf.acdc.devops.service.process.widetable;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDataSystemResourceProjectMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.repository.WideTableRepository;
import cn.xdf.acdc.devops.service.config.ACDCInnerProperties;
import cn.xdf.acdc.devops.service.config.ACDCWideTableProperties;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl.ConnectionColumnConfigurationGeneratorManager;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import cn.xdf.acdc.devops.service.process.widetable.sql.WideTableSqlService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
public class WideTableServiceTest {
    
    @MockBean
    private ACDCWideTableProperties acdcWideTableProperties;
    
    @MockBean
    private ACDCInnerProperties acdcInnerProperties;
    
    @MockBean
    private DataSystemServiceManager dataSystemServiceManager;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private ConnectionColumnConfigurationGeneratorManager connectionColumnConfigurationGeneratorManager;
    
    @MockBean
    private WideTableRepository wideTableRepository;
    
    @MockBean
    private ConnectionService connectionService;
    
    @MockBean
    private WideTableSqlService wideTableSqlService;
    
    @MockBean
    private I18nService i18n;
    
    @MockBean
    private DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService;
    
    @Mock
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Mock
    private DataSystemMetadataService starrocksDataSystemMetadataService;
    
    @Autowired
    private WideTableService wideTableService;
    
    @Test
    public void testCreateInnerConnectionIfNeededShouldReturnExistConnection() {
        WideTableDO wideTableDO = new WideTableDO(1L);
        wideTableDO.setWideTableDataSystemResourceProjectMappings(Sets.newHashSet(
                new WideTableDataSystemResourceProjectMappingDO()
                        .setId(11L)
                        .setWideTable(wideTableDO)
                        .setProject(new ProjectDO(111L))
                        .setDataSystemResource(new DataSystemResourceDO(1111L))
        ));
        Mockito.when(wideTableRepository.findById(ArgumentMatchers.eq(1L))).thenReturn(Optional.of(wideTableDO));
        ArgumentCaptor<ConnectionQuery> connectionQueryArgumentCaptor = ArgumentCaptor.forClass(ConnectionQuery.class);
        
        Mockito.when(connectionService.query(connectionQueryArgumentCaptor.capture()))
                .thenReturn(Lists.newArrayList(new ConnectionDTO().setId(11111L)));
        
        wideTableService.createInnerConnectionIfNeeded(1L);
        ArgumentCaptor<WideTableDO> wideTableCaptor = ArgumentCaptor.forClass(WideTableDO.class);
        Mockito.verify(wideTableRepository).save(wideTableCaptor.capture());
        
        ConnectionQuery query = new ConnectionQuery()
                .setSourceDataCollectionId(1111L)
                .setSpecificConfiguration("{\"INNER_CONNECTION_TYPE\":\"STARROCKS_SOURCE\"}")
                .setDeleted(false);
        Assert.assertEquals(query, connectionQueryArgumentCaptor.getValue());
        
        WideTableDO wideTable = wideTableCaptor.getValue();
        Assert.assertEquals((Long) 1L, wideTable.getId());
        WideTableDataSystemResourceProjectMappingDO resourceProjectMapping = (WideTableDataSystemResourceProjectMappingDO) wideTable.getWideTableDataSystemResourceProjectMappings().toArray()[0];
        Assert.assertEquals((Long) 111L, resourceProjectMapping.getProject().getId());
        Assert.assertEquals((Long) 1111L, resourceProjectMapping.getDataSystemResource().getId());
        ConnectionDO connectionDO = (ConnectionDO) wideTable.getConnections().toArray()[0];
        Assert.assertEquals((Long) 11111L, connectionDO.getId());
    }
    
    @Test
    public void testCreateInnerConnectionIfNeededShouldAsExpected() {
        WideTableDO wideTableDO = new WideTableDO(1L);
        wideTableDO.setWideTableDataSystemResourceProjectMappings(Sets.newHashSet(
                new WideTableDataSystemResourceProjectMappingDO()
                        .setId(11L)
                        .setWideTable(wideTableDO)
                        .setProject(new ProjectDO(111L))
                        .setDataSystemResource(new DataSystemResourceDO(1111L))
        ));
        Mockito.when(wideTableRepository.findById(ArgumentMatchers.eq(1L))).thenReturn(Optional.of(wideTableDO));
        ArgumentCaptor<ConnectionQuery> connectionQueryArgumentCaptor = ArgumentCaptor.forClass(ConnectionQuery.class);
        
        Mockito.when(connectionService.query(connectionQueryArgumentCaptor.capture()))
                .thenReturn(Lists.newArrayList());
        
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.eq(1111L)))
                .thenReturn(new DataSystemResourceDetailDTO().setId(51L).setName("table_name").setDataSystemType(DataSystemType.MYSQL));
        Mockito.when(dataSystemServiceManager.getDataSystemMetadataService(ArgumentMatchers.eq(DataSystemType.MYSQL)))
                .thenReturn(dataSystemMetadataService);
        Mockito.when(dataSystemServiceManager.getDataSystemMetadataService(ArgumentMatchers.eq(DataSystemType.STARROCKS)))
                .thenReturn(starrocksDataSystemMetadataService);
        Mockito.when(dataSystemMetadataService.getDataCollectionDefinition(ArgumentMatchers.eq(1111L)))
                .thenReturn(new DataCollectionDefinition("table_name", Lists.newArrayList(
                                new DataFieldDefinition("id", "bigint", Schema.INT64_SCHEMA, false, null, new HashMap<>(), Sets.newHashSet("primary"))
                        ))
                );
        Mockito.when(acdcWideTableProperties.getStarrocksClusterId()).thenReturn(2L);
        Mockito.when(acdcWideTableProperties.getDatabaseName()).thenReturn("db_name");
        Mockito.when(dataSystemResourceService.getChildren(ArgumentMatchers.eq(2L), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_DATABASE)))
                .thenReturn(Lists.newArrayList(
                                new DataSystemResourceDTO().setName("db_name").setId(31L),
                                new DataSystemResourceDTO().setName("db_name_1").setId(32L)
                        )
                );
        ArgumentCaptor<DataCollectionDefinition> definitionArgumentCaptor = ArgumentCaptor.forClass(DataCollectionDefinition.class);
        Mockito.when(starrocksDataSystemMetadataService.createDataCollectionByDataDefinition(
                        ArgumentMatchers.eq(31L), ArgumentMatchers.eq("table_name_51"), definitionArgumentCaptor.capture()
                )
        ).thenReturn(new DataSystemResourceDTO().setId(52L));
        Mockito.when(connectionColumnConfigurationGeneratorManager.generateConnectionColumnConfiguration(
                ArgumentMatchers.eq(51L), ArgumentMatchers.eq(52L)
        )).thenReturn(Lists.newArrayList(
                new ConnectionColumnConfigurationDTO().setSourceColumnName("id").setSinkColumnName("id"),
                new ConnectionColumnConfigurationDTO().setSinkColumnName("__is_deleted"),
                new ConnectionColumnConfigurationDTO().setSinkColumnName("__event_offset")
        ));
        Mockito.when(acdcInnerProperties.getUserDomainAccount()).thenReturn("user_account");
        Mockito.when(acdcInnerProperties.getProjectId()).thenReturn(72L);
        
        ArgumentCaptor<ArrayList> connectionsCapture = ArgumentCaptor.forClass(ArrayList.class);
        Mockito.when(connectionService.batchCreate(connectionsCapture.capture(), ArgumentMatchers.eq("user_account")))
                .thenReturn(Lists.newArrayList(new ConnectionDetailDTO().setId(88L)));
        wideTableService.createInnerConnectionIfNeeded(1L);
        ArgumentCaptor<WideTableDO> wideTableCaptor = ArgumentCaptor.forClass(WideTableDO.class);
        Mockito.verify(wideTableRepository).save(wideTableCaptor.capture());
        
        ConnectionQuery query = new ConnectionQuery()
                .setSourceDataCollectionId(1111L)
                .setSpecificConfiguration("{\"INNER_CONNECTION_TYPE\":\"STARROCKS_SOURCE\"}")
                .setDeleted(false);
        Assert.assertEquals(query, connectionQueryArgumentCaptor.getValue());
        
        WideTableDO wideTable = wideTableCaptor.getValue();
        Assert.assertEquals((Long) 1L, wideTable.getId());
        WideTableDataSystemResourceProjectMappingDO resourceProjectMapping = (WideTableDataSystemResourceProjectMappingDO) wideTable.getWideTableDataSystemResourceProjectMappings().toArray()[0];
        Assert.assertEquals((Long) 111L, resourceProjectMapping.getProject().getId());
        Assert.assertEquals((Long) 1111L, resourceProjectMapping.getDataSystemResource().getId());
        ConnectionDO connectionDO = (ConnectionDO) wideTable.getConnections().toArray()[0];
        Assert.assertEquals((Long) 88L, connectionDO.getId());
    }
    
    @Configuration
    @ComponentScan(basePackages = "cn.xdf.acdc.devops.service.process.widetable.impl")
    @EnableAspectJAutoProxy
    static class Config {
        
        @Bean
        public ObjectMapper getObjectMapper() {
            return new ObjectMapper();
        }
    }
}
