package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.repository.ConnectClusterRepository;
import cn.xdf.acdc.devops.repository.ConnectorClassRepository;
import cn.xdf.acdc.devops.repository.ConnectorConfigurationRepository;
import cn.xdf.acdc.devops.repository.ConnectorRepository;
import cn.xdf.acdc.devops.repository.DataSystemResourceRepository;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectorServiceImplTest {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Autowired
    private ConnectorClassRepository connectorClassRepository;

    @Autowired
    private ConnectClusterRepository connectClusterRepository;

    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    private ConnectorConfigurationRepository connectorConfigurationRepository;

    @Autowired
    private EntityManager entityManager;

    @MockBean
    private DataSystemServiceManager dataSystemServiceManager;

    @Mock
    private DataSystemSourceConnectorService dataSystemSourceConnectorService;

    @Mock
    private DataSystemSinkConnectorService dataSystemSinkConnectorService;

    @Before
    public void setUp() {
        when(dataSystemServiceManager.getDataSystemSourceConnectorService(any())).thenReturn(dataSystemSourceConnectorService);
        when(dataSystemServiceManager.getDataSystemSinkConnectorService(any())).thenReturn(dataSystemSinkConnectorService);
    }

    @Test
    public void testCreateShouldSaveConnectorAndConfigurations() {
        ConnectorDetailDTO toCreateConnector = connectorService.create(generateConnectorDetail("to_create_connector"));

        entityManager.flush();
        entityManager.clear();

        ConnectorDO createdConnector = connectorRepository.getOne(toCreateConnector.getId());
        Assertions.assertThat(createdConnector.getConnectorConfigurations().size()).isEqualTo(toCreateConnector.getConnectorConfigurations().size());
    }

    private ConnectorDetailDTO generateConnectorDetail(final String name) {
        List<ConnectorConfigurationDTO> connectorConfigurations = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ConnectorConfigurationDTO configuration = new ConnectorConfigurationDTO();
            configuration.setName("connector_configuration_name_" + i);
            configuration.setValue("connector_configuration_value_" + i);
            connectorConfigurations.add(configuration);
        }

        ConnectClusterDO savedConnectCluster = saveConnectCluster();
        ConnectorClassDO savedConnectorClass = saveMysqlSinkConnectorClass();
        KafkaClusterDO savedKafkaCluster = saveKafkaCluster();
        DataSystemResourceDO saveDataSystemResource = saveDataSystemResource();

        ConnectorDetailDTO toCreateConnector = new ConnectorDetailDTO();
        toCreateConnector.setName(name);
        toCreateConnector.setConnectClusterId(savedConnectCluster.getId());
        toCreateConnector.setConnectorClassId(savedConnectorClass.getId());
        toCreateConnector.setKafkaClusterId(savedKafkaCluster.getId());
        toCreateConnector.setDataSystemResourceId(saveDataSystemResource.getId());
        toCreateConnector.setActualState(ConnectorState.PENDING);
        toCreateConnector.setDesiredState(ConnectorState.RUNNING);
        toCreateConnector.setConnectorConfigurations(connectorConfigurations);

        return toCreateConnector;
    }

    private ConnectClusterDO saveConnectCluster() {
        ConnectClusterDO connectClusterDO = new ConnectClusterDO();
        connectClusterDO.setConnectRestApiUrl("");
        connectClusterDO.setVersion("");
        return connectClusterRepository.save(connectClusterDO);
    }

    private ConnectorClassDO saveMysqlSinkConnectorClass() {
        ConnectorClassDO connectorClass = new ConnectorClassDO();
        connectorClass.setDataSystemType(DataSystemType.MYSQL);
        connectorClass.setConnectorType(ConnectorType.SINK);
        connectorClass.setName("");
        connectorClass.setSimpleName("");
        return connectorClassRepository.save(connectorClass);
    }

    private ConnectorClassDO saveMysqlSourceConnectorClass() {
        ConnectorClassDO connectorClass = new ConnectorClassDO();
        connectorClass.setDataSystemType(DataSystemType.MYSQL);
        connectorClass.setConnectorType(ConnectorType.SOURCE);
        connectorClass.setName("");
        connectorClass.setSimpleName("");
        return connectorClassRepository.save(connectorClass);
    }

    private KafkaClusterDO saveKafkaCluster() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setBootstrapServers("");
        kafkaCluster.setName("");
        kafkaCluster.setVersion("");
        return kafkaClusterRepository.save(kafkaCluster);
    }

    private DataSystemResourceDO saveDataSystemResource() {
        DataSystemResourceDO dataSystemResource = new DataSystemResourceDO();
        dataSystemResource.setName("");
        dataSystemResource.setDataSystemType(DataSystemType.MYSQL);
        dataSystemResource.setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        return dataSystemResourceRepository.save(dataSystemResource);
    }

    @Test
    public void testUpdateParticularConfigurationShouldAsExpect() {
        ConnectorDetailDTO createdConnector = connectorService.create(generateConnectorDetail("to_create_connector"));

        entityManager.flush();
        entityManager.clear();

        Map<String, String> toUpdateConnectorConfigurations = new HashMap();
        for (int i = 0; i < 2; i++) {
            toUpdateConnectorConfigurations.put("connector_configuration_name_" + i, "updated_connector_configuration_value_" + i);
        }

        connectorService.updateParticularConfiguration(createdConnector.getId(), toUpdateConnectorConfigurations);

        // assert
        ConnectorDO updatedConnector = connectorRepository.getOne(createdConnector.getId());
        Map<String, String> actualConfigurations = new HashMap<>();
        createdConnector.getConnectorConfigurations().forEach(each -> actualConfigurations.put(each.getName(), each.getValue()));
        actualConfigurations.putAll(toUpdateConnectorConfigurations);

        Map<String, String> updatedConfigurations = updatedConnector.getConnectorConfigurations().stream()
                .collect(Collectors.toMap(ConnectorConfigurationDO::getName, ConnectorConfigurationDO::getValue));
        Assertions.assertThat(updatedConfigurations).isEqualTo(actualConfigurations);
    }

    @Test
    public void testUpdateEntireConfigurationShouldAsExpect() {
        ConnectorDetailDTO createdConnector = connectorService.create(generateConnectorDetail("to_create_connector"));

        entityManager.flush();
        entityManager.clear();

        Map<String, String> toUpdateConnectorConfigurations = new HashMap();
        for (int i = 0; i < 2; i++) {
            toUpdateConnectorConfigurations.put("updated_connector_configuration_name_" + i, "updated_connector_configuration_value_" + i);
        }

        connectorService.updateEntireConfiguration(createdConnector.getId(), toUpdateConnectorConfigurations);

        // assert
        Assertions.assertThat(connectorConfigurationRepository.count()).isEqualTo(toUpdateConnectorConfigurations.size());
        ConnectorDO updatedConnector = connectorRepository.getOne(createdConnector.getId());
        Map<String, String> updatedConfigurations = updatedConnector.getConnectorConfigurations().stream()
                .collect(Collectors.toMap(ConnectorConfigurationDO::getName, ConnectorConfigurationDO::getValue));
        Assertions.assertThat(updatedConfigurations).isEqualTo(toUpdateConnectorConfigurations);
    }

    @Test
    public void testUpdateActualStateShouldAsExpect() {
        ConnectorDetailDTO createdConnector = connectorService.create(generateConnectorDetail("to_create_connector"));
        connectorService.updateActualState(createdConnector.getId(), ConnectorState.RUNNING);

        // assert
        ConnectorDO updatedConnector = connectorRepository.getOne(createdConnector.getId());
        Assertions.assertThat(updatedConnector.getActualState()).isEqualTo(ConnectorState.RUNNING);
    }

    @Test
    public void testQueryShouldAsExpect() {
        for (int i = 0; i < 5; i++) {
            connectorService.create(generateConnectorDetail("to_create_connector_" + i));
        }
        ConnectorQuery query = new ConnectorQuery();
        query.setName("create");

        List<ConnectorDTO> queriedConnectors = connectorService.query(query);
        Assertions.assertThat(queriedConnectors.size()).isEqualTo(5);
    }

    @Test
    public void testQueryDetailShouldAsExpect() {
        for (int i = 0; i < 5; i++) {
            connectorService.create(generateConnectorDetail("to_create_connector_" + i));
        }
        ConnectorQuery query = new ConnectorQuery();
        query.setName("create");

        List<ConnectorDetailDTO> queriedConnectors = connectorService.queryDetail(query);
        Assertions.assertThat(queriedConnectors.size()).isEqualTo(5);
    }

    @Test
    public void testQueryDetailWithDecryptConfigurationShouldAsExpect() {
        Set<String> sensitiveConfigurationNames = Sets.newHashSet("connector_configuration_name_0", "connector_configuration_name_1");
        when(dataSystemSourceConnectorService.getSensitiveConfigurationNames()).thenReturn(sensitiveConfigurationNames);
        when(dataSystemSinkConnectorService.getSensitiveConfigurationNames()).thenReturn(sensitiveConfigurationNames);

        Map<String, String> nameToDecryptValues = new HashMap<>();
        // mock a mysql source connector
        ConnectorDetailDTO toCreateConnectorDetail = generateConnectorDetail("to_decrypt_configuration_source");
        toCreateConnectorDetail.getConnectorConfigurations().forEach(each -> {
            nameToDecryptValues.put(each.getName(), each.getValue());
            if (sensitiveConfigurationNames.contains(each.getName())) {
                each.setValue(EncryptUtil.encrypt(each.getValue()));
            }
        });
        connectorService.create(toCreateConnectorDetail);

        // mock a mysql sink connector
        ConnectorDetailDTO otherToCreateConnectorDetail = generateConnectorDetail("to_decrypt_configuration_sink");
        otherToCreateConnectorDetail.setConnectorClassId(saveMysqlSourceConnectorClass().getId());
        otherToCreateConnectorDetail.getConnectorConfigurations().forEach(each -> {
            if (sensitiveConfigurationNames.contains(each.getName())) {
                each.setValue(EncryptUtil.encrypt(each.getValue()));
            }
        });
        connectorService.create(otherToCreateConnectorDetail);

        entityManager.flush();
        entityManager.clear();

        ConnectorQuery query = new ConnectorQuery();
        query.setName("to_decrypt_configuration_");

        // envoke
        List<ConnectorDetailDTO> queriedConnectors = connectorService.queryDetailWithDecryptConfiguration(query);

        // assert
        Assertions.assertThat(queriedConnectors).hasSize(2);

        queriedConnectors.forEach(each -> {
            each.getConnectorConfigurations().forEach(eachConfiguration -> {
                Assertions.assertThat(eachConfiguration.getValue()).isEqualTo(nameToDecryptValues.get(eachConfiguration.getName()));
            });
        });
    }

    @Test
    public void testPagedQueryShouldAsExpect() {
        for (int i = 0; i < 5; i++) {
            connectorService.create(generateConnectorDetail("to_create_connector_" + i));
        }
        ConnectorQuery query = new ConnectorQuery();
        query.setName("create");
        query.setCurrent(2);
        query.setPageSize(1);

        Page<ConnectorDTO> queriedConnectors = connectorService.pagedQuery(query);
        Assertions.assertThat(queriedConnectors.getTotalElements()).isEqualTo(5);
        Assertions.assertThat(queriedConnectors.getTotalPages()).isEqualTo(5);
        Assertions.assertThat(queriedConnectors.getPageable().getPageNumber()).isEqualTo(query.getCurrent() - 1);
        Assertions.assertThat(queriedConnectors.getPageable().getPageSize()).isEqualTo(query.getPageSize());
    }

    @Test
    public void testStartShouldUpdateDesiredStateToRunning() {
        // update connector desired state to running
        ConnectorDetailDTO createdConnector = connectorService.create(generateConnectorDetail("to_create_connector"));
        connectorService.stop(createdConnector.getId());
        connectorService.start(createdConnector.getId());

        // assert
        ConnectorDO updatedConnector = connectorRepository.getOne(createdConnector.getId());
        Assertions.assertThat(updatedConnector.getDesiredState()).isEqualTo(ConnectorState.RUNNING);
    }

    @Test
    public void testStopShouldUpdateDesiredStateToStopped() {
        // update connector desired state to stopped
        ConnectorDetailDTO createdConnector = connectorService.create(generateConnectorDetail("to_create_connector"));
        connectorService.stop(createdConnector.getId());

        // assert
        ConnectorDO updatedConnector = connectorRepository.getOne(createdConnector.getId());
        Assertions.assertThat(updatedConnector.getDesiredState()).isEqualTo(ConnectorState.STOPPED);
    }

    @Test
    public void testGetByIdShouldAsExpect() {
        ConnectorDetailDTO createdConnector = connectorService.create(generateConnectorDetail("to_create_connector"));
        ConnectorDetailDTO resultConnector = connectorService.getDetailById(createdConnector.getId());
        Assertions.assertThat(resultConnector.getName()).isEqualTo(createdConnector.getName());
    }
}
