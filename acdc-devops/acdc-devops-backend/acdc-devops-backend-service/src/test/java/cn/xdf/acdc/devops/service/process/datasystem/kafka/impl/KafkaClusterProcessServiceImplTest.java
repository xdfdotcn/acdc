package cn.xdf.acdc.devops.service.process.datasystem.kafka.impl;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaClusterProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaTopicProcessService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperServiceImpl;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaClusterProcessServiceImplTest {

    @Autowired
    private KafkaClusterProcessService kafkaClusterProcessService;

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @MockBean
    private KafkaHelperServiceImpl kafkaHelperService;

    @Autowired
    private KafkaTopicProcessService kafkaTopicProcessService;

    @Autowired
    private ProjectRepository projectRepository;

    @Before
    public void init() {
        initKafkaCluster();
    }

    @Test
    public void testGetKafkaClusterWithAnalysisConfig() {
        KafkaClusterDTO cluster = kafkaClusterProcessService.getKafkaClusterWithFlatConfig(1L);
        Assertions.assertThat(cluster.getSaslMechanism()).isEqualTo("SCRAM-SHA-512");
        Assertions.assertThat(cluster.getSecurityProtocol()).isEqualTo("SASL_PLAINTEXT");
    }

    @Test
    public void testQueryKafkaClustersByName() {
        KafkaClusterQuery query = KafkaClusterQuery.builder().name("mock").projectId(1L).build();
        Page<KafkaClusterDTO> page = kafkaClusterProcessService.queryByProject(query);
        Assertions.assertThat(page.get().count()).isEqualTo(1);
    }

    @Test
    public void testQueryKafkaClustersByProject() {
        KafkaClusterQuery query = KafkaClusterQuery.builder().projectId(1L).build();
        query.setCurrent(1);
        query.setPageSize(10);
        Page<KafkaClusterDTO> page = kafkaClusterProcessService.queryByProject(query);
        Assertions.assertThat(page.get().count()).isEqualTo(2);
    }

    @Test(expected = ClientErrorException.class)
    public void testShouldThrowExceptionWhenQueryKafkaClustersWithoutProjectId() {
        KafkaClusterQuery query = KafkaClusterQuery.builder().name("测试").build();
        Page<KafkaClusterDTO> page = kafkaClusterProcessService.queryByProject(query);
        Assertions.assertThat(page.get().count()).isEqualTo(2);
    }

    @Test
    public void testShouldReturnEmptyWhenHasNoKafkaClusterRelatedToProject() {
        KafkaClusterQuery query = KafkaClusterQuery.builder().projectId(2L).build();
        Page<KafkaClusterDTO> page = kafkaClusterProcessService.queryByProject(query);
        Assertions.assertThat(page.get().count()).isEqualTo(0);
    }

    @Test
    @Transactional
    public void testSaveKafkaCluster() {
        when(kafkaHelperService.listTopics(any())).thenReturn(Sets.newHashSet("mock-topic-1", "mock-topic-2", "mock-topic-3", "mock-topic-4"));
        // 跳过配置校验
        doNothing().when(kafkaHelperService).checkConfig(anyString(), any());
        kafkaClusterProcessService.saveKafkaClusterAndSyncKafkaClusterTopic(buildKafkaClusterDTO());
        KafkaClusterDTO kafkaCluster = kafkaClusterProcessService.getKafkaCluster(3L);
        Assertions.assertThat(kafkaCluster).isNotNull();
        Page<KafkaTopicDTO> page = kafkaTopicProcessService.queryKafkaTopic(KafkaTopicDTO.builder().kafkaClusterId(3L).build());
        Assertions.assertThat(page.getTotalElements()).isEqualTo(4);
        Optional<ProjectDO> project = projectRepository.findById(1L);
        Assertions.assertThat(project.get().getKafkaClusters().size()).isEqualTo(3);

    }

    @Test(expected = ClientErrorException.class)
    public void testShouldThrowExceptionWhenSaveKafkaClusterWithWrongConfig() {
        // 执行配置校验
        Mockito.doCallRealMethod().when(kafkaHelperService).checkConfig(anyString(), any());
        kafkaClusterProcessService.saveKafkaClusterAndSyncKafkaClusterTopic(buildKafkaClusterDTO());
    }

    @Test
    public void testUpdateKafkaCluster() {
        when(kafkaHelperService.listTopics(any())).thenReturn(Sets.newHashSet("mock-topic-1", "mock-topic-2", "mock-topic-3"));
        doNothing().when(kafkaHelperService).checkConfig(anyString(), any());
        KafkaClusterDTO dto = buildKafkaClusterDTO();
        dto.setName("测试集群-1");
        dto.setId(1L);
        kafkaClusterProcessService.updateKafkaCluster(dto);
        KafkaClusterDTO kafkaCluster = kafkaClusterProcessService.getKafkaCluster(1L);
        Assertions.assertThat(kafkaCluster.getName()).isEqualTo("测试集群-1");
        Page<KafkaTopicDTO> page = kafkaTopicProcessService.queryKafkaTopic(KafkaTopicDTO.builder().kafkaClusterId(1L).build());
        Assertions.assertThat(page.getTotalElements()).isEqualTo(3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShouldThrowExceptionIfIdIsMissingWhenUpdateKafkaCluster() {
        doNothing().when(kafkaHelperService).checkConfig(anyString(), any());
        KafkaClusterDTO dto = buildKafkaClusterDTO();
        kafkaClusterProcessService.updateKafkaCluster(dto);
    }

    private KafkaClusterDTO buildKafkaClusterDTO() {
        String securityConfiguration = "{\"security.protocol\":\"SASL_PLAINTEXT\",\"sasl.mechanism\":\"SCRAM-SHA-512\",\"sasl.jaas.config\":\"/wCQKba+5cqYnkr9cdEsdLM8IJYkjAoFBhFPNEZOfiq9bAHhRuBiyyQPgEnEgMX+lajceECYiyaSSZN7AHCBNu+xeivPqDkfyAu3L/cjMG/zoowUh/ZoTsh75SP+JtsVswEu/hdwQ6pg1ZLTB4Zpsw==\"}";
        return KafkaClusterDTO.builder().securityConfiguration(securityConfiguration).clusterType(KafkaClusterType.USER)
                .name("测试集群").description("test").version("2.6.0").bootstrapServers("127.0.0.1:9092").projectId(1L).build();
    }

    private void initKafkaCluster() {
        KafkaClusterDO kafkaCluster1 = buildKafkaClusterDTO().toKafkaClusterDO();
        kafkaCluster1.setId(1L);
        KafkaClusterDO kafkaCluster2 = buildKafkaClusterDTO().toKafkaClusterDO();
        kafkaCluster2.setId(2L);
        kafkaCluster2.setName("mock集群");
        kafkaClusterRepository.saveAll(Lists.newArrayList(kafkaCluster1, kafkaCluster2));

        ProjectDO project1 = ProjectDO.builder().id(1L).name("test project-1").description("for mock").build();
        project1.setKafkaClusters(Sets.newHashSet(kafkaCluster1, kafkaCluster2));
        ProjectDO project2 = ProjectDO.builder().id(2L).name("test project-2").description("for mock").build();
        projectRepository.saveAll(Lists.newArrayList(project1, project2));
    }
}
