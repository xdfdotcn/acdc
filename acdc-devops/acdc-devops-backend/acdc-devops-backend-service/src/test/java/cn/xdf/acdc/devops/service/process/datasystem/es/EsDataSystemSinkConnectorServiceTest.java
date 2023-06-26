package cn.xdf.acdc.devops.service.process.datasystem.es;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DefaultConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.EsHelperService;

@RunWith(SpringRunner.class)
public class EsDataSystemSinkConnectorServiceTest {

    private EsDataSystemSinkConnectorServiceImpl esDataSystemSinkConnectorServiceImpl;

    @Mock
    private ConnectorClassService connectorClassService;

    @Mock
    private DataSystemResourceService dataSystemResourceService;

    @Mock
    private ConnectionService connectionService;

    @Mock
    private EsHelperService helperService;

    @Before
    public void setUp() throws Exception {
        esDataSystemSinkConnectorServiceImpl = new EsDataSystemSinkConnectorServiceImpl();

        ReflectionTestUtils.setField(
                esDataSystemSinkConnectorServiceImpl,
                "connectionService",
                connectionService);

        ReflectionTestUtils.setField(
                esDataSystemSinkConnectorServiceImpl,
                "dataSystemResourceService",
                dataSystemResourceService);

        ReflectionTestUtils.setField(
                esDataSystemSinkConnectorServiceImpl,
                "connectorClassService",
                connectorClassService);

        ReflectionTestUtils.setField(
                esDataSystemSinkConnectorServiceImpl,
                "helperService",
                helperService);
    }

    @Test
    public void testBeforeConnectorCreation() {
        esDataSystemSinkConnectorServiceImpl.beforeConnectorCreation(1L);
    }

    @Test
    public void testGetConnectorDefaultConfiguration() {
        Map<String, String> expectedDefaultConfiguration = new HashMap<>();
        expectedDefaultConfiguration.put("default-configuration-name-0", "default-configuration-value-0");
        expectedDefaultConfiguration.put("default-configuration-name-1", "default-configuration-value-1");

        Set<DefaultConnectorConfigurationDTO> defaultConnectorConfigurations = new HashSet<>();
        expectedDefaultConfiguration.forEach((key, value) -> {
            defaultConnectorConfigurations.add(
                    new DefaultConnectorConfigurationDTO()
                            .setName(key)
                            .setValue(value)
            );
        });

        ConnectorClassDetailDTO connectorClassDetail = new ConnectorClassDetailDTO()
                .setDefaultConnectorConfigurations(defaultConnectorConfigurations);
        when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(
                eq(DataSystemType.ELASTIC_SEARCH),
                eq(ConnectorType.SINK))
        ).thenReturn(connectorClassDetail);

        Map<String, String> defaultConfiguration = esDataSystemSinkConnectorServiceImpl
                .getConnectorDefaultConfiguration();

        Assertions.assertThat(defaultConfiguration).isEqualTo(expectedDefaultConfiguration);
    }

    @Test
    public void testGenerateConnectorCustomConfiguration() {
        ConnectionDetailDTO connectionDetail = TestHelper.createConnectionDetail();
        DataSystemResourceDTO sourceDataCollection = TestHelper.createSourceDataCollection();
        DataSystemResourceDTO sinkIndex = TestHelper.createIndexRs();
        DataSystemResourceDetailDTO sinkClusterDetail = TestHelper.createClusterRsDetal();

        when(connectionService.getDetailById(eq(connectionDetail.getId())))
                .thenReturn(connectionDetail);

        when(dataSystemResourceService.getById(eq(connectionDetail.getSourceDataCollectionId())))
                .thenReturn(sourceDataCollection);

        when(dataSystemResourceService.getById(eq(connectionDetail.getSinkDataCollectionId())))
                .thenReturn(sinkIndex);

        when(dataSystemResourceService.getDetailParent(eq(connectionDetail.getSinkDataCollectionId()),
                eq(DataSystemResourceType.ELASTIC_SEARCH_CLUSTER)))
                        .thenReturn(sinkClusterDetail);

        final Map<String, String> customConfiguration = esDataSystemSinkConnectorServiceImpl
                .generateConnectorCustomConfiguration(connectionDetail.getId());

        Map<String, String> desiredCustomConfiguration = new HashMap<>();
        desiredCustomConfiguration.put("topics", "topic_name");
        desiredCustomConfiguration.put("connection.username", TestHelper.U_PASSWORD.getUsername());
        desiredCustomConfiguration.put("connection.password", TestHelper.U_PASSWORD.getPassword());
        desiredCustomConfiguration.put("connection.url", TestHelper.NODE_SERVERS);
        desiredCustomConfiguration.put("transforms.replaceField.whitelist", "NONE");
        desiredCustomConfiguration.put("transforms.replaceField.whitelist", "source_column_name");
        desiredCustomConfiguration.put("transforms.replaceField.renames", "source_column_name:sink_column_name");
        desiredCustomConfiguration.put("transforms.topicToIndex.replacement", "index1");

        Assertions.assertThat(customConfiguration).isEqualTo(desiredCustomConfiguration);
    }

    @Test
    public void testGetConnectorSpecificConfigurationDefinitionsShouldReturnEmptyList() {
        Assertions.assertThat(esDataSystemSinkConnectorServiceImpl
                .getConnectorSpecificConfigurationDefinitions()).isEmpty();
    }

    @Test
    public void testGetSensitiveConfigurationNames() {
        Assertions.assertThat(esDataSystemSinkConnectorServiceImpl
                .getSensitiveConfigurationNames())
                .isEqualTo(Sets.newHashSet("connection.password"));
    }

    @Test
    public void testGetDataSystemTypeShouldReturnEs() {
        Assertions.assertThat(esDataSystemSinkConnectorServiceImpl
                .getDataSystemType()).isEqualTo(DataSystemType.ELASTIC_SEARCH);
    }
}
