package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.CommonClientConfigs;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDataSystemMetadataServiceImplTest {
    
    @Autowired
    @Qualifier("kafkaDataSystemMetadataServiceImpl")
    private DataSystemMetadataService kafkaDataSystemMetadataService;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private KafkaHelperService kafkaHelperService;
    
    @Test
    public void testGetDataCollectionDefinitionShouldReturnEmptyFieldList() {
        when(dataSystemResourceService.getById(eq(1L))).thenReturn(new DataSystemResourceDTO().setId(1L).setName("topic"));
        
        DataCollectionDefinition topicDefinition = kafkaDataSystemMetadataService.getDataCollectionDefinition(1L);
        
        Assertions.assertThat(topicDefinition.getName()).isEqualTo("topic");
        Assertions.assertThat(topicDefinition.getLowerCaseNameToDataFieldDefinitions()).isEmpty();
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenAdminClientConfigurationIsCorrect() {
        when(dataSystemResourceService.getDetailById(eq(1L))).thenReturn(generateClusterDetail());
        
        kafkaDataSystemMetadataService.checkDataSystem(1L);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenAdminClientConfigurationIsWrong() {
        when(dataSystemResourceService.getDetailById(eq(1L))).thenReturn(generateClusterDetail());
        doThrow(ServerErrorException.class).when(kafkaHelperService).checkAdminClientConfig(anyMap());
        
        kafkaDataSystemMetadataService.checkDataSystem(1L);
    }
    
    @Test
    public void testRefreshDynamicDataSystemResourceShouldPass() {
        when(dataSystemResourceService.getDetailById(eq(1L))).thenReturn(generateClusterDetail());
        Set<String> topics = Sets.newHashSet("topic_1", "topic_2", "topic_3");
        when(kafkaHelperService.listTopics(anyMap())).thenReturn(topics);
        
        kafkaDataSystemMetadataService.refreshDynamicDataSystemResource(1L);
        
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> captor = ArgumentCaptor.forClass(List.class);
        verify(dataSystemResourceService).mergeAllChildrenByName(captor.capture(), eq(DataSystemResourceType.KAFKA_TOPIC), eq(1L));
        
        captor.getValue().forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.KAFKA_TOPIC);
        });
        
        Set<String> capturedTopics = captor.getValue().stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toSet());
        Assertions.assertThat(capturedTopics).isEqualTo(topics);
    }
    
    @Test
    public void testRefreshDynamicDataSystemResourceShouldUseClusterProjectRelation() {
        DataSystemResourceDetailDTO kafkaCluster = generateClusterDetail();
        List<ProjectDTO> projects = generateProjects();
        kafkaCluster.setProjects(projects);
        when(dataSystemResourceService.getDetailById(eq(1L))).thenReturn(kafkaCluster);
        
        Set<String> topics = Sets.newHashSet("topic_1", "topic_2", "topic_3");
        when(kafkaHelperService.listTopics(anyMap())).thenReturn(topics);
        
        kafkaDataSystemMetadataService.refreshDynamicDataSystemResource(1L);
        
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> captor = ArgumentCaptor.forClass(List.class);
        verify(dataSystemResourceService).mergeAllChildrenByName(captor.capture(), eq(DataSystemResourceType.KAFKA_TOPIC), eq(1L));
        
        captor.getValue().forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.KAFKA_TOPIC);
            Assertions.assertThat(each.getProjects()).isEqualTo(projects);
        });
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnKafka() {
        Assertions.assertThat(kafkaDataSystemMetadataService.getDataSystemType()).isEqualTo(DataSystemType.KAFKA);
    }
    
    private DataSystemResourceDetailDTO generateClusterDetail() {
        DataSystemResourceConfigurationDTO securityProtocol = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.SECURITY_PROTOCOL_CONFIG.getName())
                .setValue("SASL_PLAINTEXT");
        
        DataSystemResourceConfigurationDTO mechanism = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.SASL_MECHANISM.getName())
                .setValue("SCRAM-SHA-512");
        
        DataSystemResourceConfigurationDTO username = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.USERNAME.getName())
                .setValue("user_name");
        
        DataSystemResourceConfigurationDTO encryptedPassword = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.PASSWORD.getName())
                .setValue(EncryptUtil.encrypt("password"));
        
        DataSystemResourceConfigurationDTO bootstrapServers = new DataSystemResourceConfigurationDTO()
                .setName(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                .setValue("6.6.6.2:6662");
        
        Map<String, DataSystemResourceConfigurationDTO> configurations = new HashMap<>();
        configurations.put(securityProtocol.getName(), securityProtocol);
        configurations.put(mechanism.getName(), mechanism);
        configurations.put(username.getName(), username);
        configurations.put(encryptedPassword.getName(), encryptedPassword);
        configurations.put(bootstrapServers.getName(), bootstrapServers);
        
        DataSystemResourceDetailDTO kafkaClusterDetail = new DataSystemResourceDetailDTO();
        kafkaClusterDetail.setDataSystemResourceConfigurations(configurations);
        kafkaClusterDetail.setResourceType(DataSystemResourceType.KAFKA_CLUSTER);
        
        return kafkaClusterDetail;
    }
    
    private List<ProjectDTO> generateProjects() {
        List<ProjectDTO> projects = new ArrayList<>();
        
        for (int i = 0; i < 3; i++) {
            projects.add(
                    new ProjectDTO()
                            .setId(Long.valueOf(i))
                            .setName("project_" + i)
            );
        }
        
        return projects;
    }
}
