package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationConstant;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemSinkConnectorServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class AbstractDataSystemSinkConnectorServiceTest {
    
    private AbstractDataSystemSinkConnectorService dataSystemSinkConnectorService = new HiveDataSystemSinkConnectorServiceImpl();
    
    @Mock
    private ConnectionService connectionService;
    
    @Mock
    private DataSystemResourceService dataSystemResourceService;
    
    @Before
    public void setUp() throws Exception {
        ReflectionTestUtils.setField(dataSystemSinkConnectorService, AbstractDataSystemSinkConnectorService.class, "connectionService", connectionService, null);
        ReflectionTestUtils.setField(dataSystemSinkConnectorService, AbstractDataSystemSinkConnectorService.class, "dataSystemResourceService", dataSystemResourceService, null);
    }
    
    @Test
    public void testGenerateConnectorName() {
        final ConnectionDTO connection = new ConnectionDTO().setId(1L).setSourceDataCollectionId(2L).setSinkDataCollectionId(3L);
        
        DataSystemResourceDTO mysqlCluster = new DataSystemResourceDTO().setId(11L).setDataSystemType(DataSystemType.MYSQL).setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDTO mysqlDatabase = new DataSystemResourceDTO().setName("db1");
        DataSystemResourceDTO mysqlTable = new DataSystemResourceDTO().setName("tb1");
        
        mysqlTable.setParentResource(mysqlDatabase);
        mysqlDatabase.setParentResource(mysqlCluster);
        
        DataSystemResourceDTO hive = new DataSystemResourceDTO().setId(12L).setDataSystemType(DataSystemType.HIVE).setResourceType(DataSystemResourceType.HIVE);
        DataSystemResourceDTO hiveDatabase = new DataSystemResourceDTO().setName("db2");
        DataSystemResourceDTO hiveTable = new DataSystemResourceDTO().setName("tb2");
        hiveTable.setParentResource(hiveDatabase);
        hiveDatabase.setParentResource(hive);
        
        when(connectionService.getById(connection.getId())).thenReturn(connection);
        
        when(dataSystemResourceService.getById(connection.getSourceDataCollectionId())).thenReturn(mysqlTable);
        
        when(dataSystemResourceService.getById(connection.getSinkDataCollectionId())).thenReturn(hiveTable);
        
        String expectSinkConnectorName = "sink-hive-12-db2-tb2-source-mysql-11-db1-tb1";
        
        String sinkConnectorName = dataSystemSinkConnectorService.generateConnectorName(connection.getId());
        
        Assertions.assertThat(sinkConnectorName).isEqualTo(expectSinkConnectorName);
    }
    
    @Test
    public void testGetPathFormatById() {
        DataSystemResourceDTO mysqlClusterDataSystemResource = new DataSystemResourceDTO()
                .setName("tb1")
                .setId(5L)
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        
        DataSystemResourceDTO mysqlDatabaseDataSystemResource = new DataSystemResourceDTO().setName("db1").setId(7L);
        
        DataSystemResourceDTO mysqlTableDataSystemResource = new DataSystemResourceDTO().setName("tb1").setId(6L);
        
        mysqlTableDataSystemResource.setParentResource(mysqlDatabaseDataSystemResource);
        mysqlDatabaseDataSystemResource.setParentResource(mysqlClusterDataSystemResource);
        
        when(dataSystemResourceService.getById(any())).thenReturn(mysqlTableDataSystemResource);
        
        String expectPathFormat = new StringBuilder()
                .append(mysqlClusterDataSystemResource.getDataSystemType().name().toLowerCase())
                .append(Symbol.CABLE)
                .append(mysqlClusterDataSystemResource.getId())
                .append(Symbol.CABLE)
                .append(mysqlDatabaseDataSystemResource.getName())
                .append(Symbol.CABLE)
                .append(mysqlTableDataSystemResource.getName())
                .toString();
        
        String pathFormat = dataSystemSinkConnectorService.getPathFormatById(mysqlTableDataSystemResource.getId());
        
        Assertions.assertThat(pathFormat).isEqualTo(expectPathFormat);
    }
    
    @Test
    public void testGetPathFormatByIdWhenNoParentResource() {
        DataSystemResourceDTO dataSystemResource = new DataSystemResourceDTO().setName("tb1")
                .setId(9L)
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_TABLE);
        
        when(dataSystemResourceService.getById(dataSystemResource.getId())).thenReturn(dataSystemResource);
        
        String expectPathFormat = new StringBuilder()
                .append(dataSystemResource.getDataSystemType().name().toLowerCase())
                .append(Symbol.CABLE)
                .append(dataSystemResource.getId())
                .toString();
        
        String pathFormat = dataSystemSinkConnectorService.getPathFormatById(dataSystemResource.getId());
        
        Assertions.assertThat(pathFormat).isEqualTo(expectPathFormat);
    }
    
    @Test
    public void testGenerateDestinationsConfigurationShouldAsExpect() {
        // every configuration have value, include metadata and normal field
        // all meta defined in ConnectionColumnConfigurationConstant.class
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        
        connectionColumnConfigurations.addAll(generateNormalColumnConfigurations(3));
        connectionColumnConfigurations.addAll(generateFilteredColumnConfigurations(2));
        connectionColumnConfigurations.addAll(generateMetadataColumnConfigurations());
        
        final Map<String, String> generatedConfigurations = dataSystemSinkConnectorService.generateDestinationsConfiguration("table", connectionColumnConfigurations);
        
        Map<String, String> desiredConfigurations = new HashMap<>();
        desiredConfigurations.put("destinations.table.fields.mapping",
                "source_normal_column_0:sink_normal_column_0," + "source_normal_column_1:sink_normal_column_1," + "source_normal_column_2:sink_normal_column_2,"
                        + "source_filtered_column_0:sink_filtered_column_0," + "source_filtered_column_1:sink_filtered_column_1," + "__kafka_record_offset:sink_offset_column");
        desiredConfigurations.put("destinations.table.fields.whitelist",
                "source_normal_column_0," + "source_normal_column_1," + "source_normal_column_2," + "source_filtered_column_0," + "source_filtered_column_1");
        
        // logical deletion
        desiredConfigurations.put("destinations.table.delete.mode", DeletionMode.LOGICAL.name());
        desiredConfigurations.put("destinations.table.delete.logical.field.name", "sink_logical_deletion_column");
        desiredConfigurations.put("destinations.table.delete.logical.field.value.deleted", "1");
        desiredConfigurations.put("destinations.table.delete.logical.field.value.normal", "0");
        
        // field add
        desiredConfigurations.put("destinations.table.fields.add", "sink_data_time_column:${datetime}");
        
        // row filter
        desiredConfigurations.put("destinations.table.row.filter", "source_filtered_column_0 > 0 and source_filtered_column_1 > 1");
        
        Assertions.assertThat(generatedConfigurations).isEqualTo(desiredConfigurations);
    }
    
    private List<ConnectionColumnConfigurationDTO> generateNormalColumnConfigurations(final int size) {
        List<ConnectionColumnConfigurationDTO> configurations = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName("source_normal_column_" + i).setSinkColumnName("sink_normal_column_" + i));
        }
        return configurations;
    }
    
    private List<ConnectionColumnConfigurationDTO> generateFilteredColumnConfigurations(final int size) {
        List<ConnectionColumnConfigurationDTO> configurations = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName("source_filtered_column_" + i).setSinkColumnName("sink_filtered_column_" + i).setFilterOperator(">")
                    .setFilterValue(i + ""));
        }
        return configurations;
    }
    
    private List<ConnectionColumnConfigurationDTO> generateMetadataColumnConfigurations() {
        List<ConnectionColumnConfigurationDTO> configurations = new ArrayList<>();
        
        configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName(ConnectionColumnConfigurationConstant.META_LOGICAL_DEL).setSinkColumnName("sink_logical_deletion_column"));
        
        configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName(ConnectionColumnConfigurationConstant.META_DATE_TIME).setSinkColumnName("sink_data_time_column"));
        
        configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName(ConnectionColumnConfigurationConstant.META_KAFKA_RECORD_OFFSET).setSinkColumnName("sink_offset_column"));
        
        configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName(SystemConstant.EMPTY_STRING).setSinkColumnName("sink_none_column"));
        
        return configurations;
    }
    
    @Test
    public void testGenerateDestinationsConfigurationShouldHaveNoFieldAddConfigurationWhenNoFieldAdded() {
        // build a List<ConnectionColumnConfigurationDTO> with no field add
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        
        connectionColumnConfigurations.addAll(generateNormalColumnConfigurations(3));
        connectionColumnConfigurations.addAll(generateFilteredColumnConfigurations(2));
        
        Map<String, String> generatedConfigurations = dataSystemSinkConnectorService.generateDestinationsConfiguration("table", connectionColumnConfigurations);
        
        Assertions.assertThat(generatedConfigurations.get("destinations.table.fields.add")).isNull();
    }
    
    @Test
    public void testGenerateDestinationsConfigurationShouldHaveNoWhiteListConfigurationWhenNoFieldMapping() {
        // build a List<ConnectionColumnConfigurationDTO> with no field mapping
        List<ConnectionColumnConfigurationDTO> configurations = new ArrayList<>();
        
        configurations.add(new ConnectionColumnConfigurationDTO().setSourceColumnName(SystemConstant.EMPTY_STRING).setSinkColumnName("sink_none_column"));
        
        Map<String, String> generatedConfigurations = dataSystemSinkConnectorService.generateDestinationsConfiguration("table", configurations);
        Assertions.assertThat(generatedConfigurations).isEmpty();
    }
    
    @Test
    public void testGenerateDestinationsConfigurationShouldHaveNoRowFilterExpressionConfigurationWhenNoRowFilterExpression() {
        // build a List<ConnectionColumnConfigurationDTO> with no row filter
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        
        connectionColumnConfigurations.addAll(generateNormalColumnConfigurations(3));
        connectionColumnConfigurations.addAll(generateMetadataColumnConfigurations());
        
        Map<String, String> generatedConfigurations = dataSystemSinkConnectorService.generateDestinationsConfiguration("table", connectionColumnConfigurations);
        
        Assertions.assertThat(generatedConfigurations.get("destinations.table.row.filter")).isNull();
    }
    
    @Test
    public void testGenerateDestinationsConfigurationShouldHaveNoLogicalDeletionConfigurationWhenNoLogicalDeletion() {
        // build a List<ConnectionColumnConfigurationDTO> without logical deletion
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        
        connectionColumnConfigurations.addAll(generateNormalColumnConfigurations(3));
        connectionColumnConfigurations.addAll(generateFilteredColumnConfigurations(2));
        
        Map<String, String> generatedConfigurations = dataSystemSinkConnectorService.generateDestinationsConfiguration("table", connectionColumnConfigurations);
        
        Assertions.assertThat(generatedConfigurations.get("destinations.table.delete.mode")).isNull();
    }
}
