package cn.xdf.acdc.devops.service.process.kafka.impl;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.query.KafkaTopicQuery;
import cn.xdf.acdc.devops.repository.DataSystemResourceRepository;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.repository.KafkaTopicRepository;
import cn.xdf.acdc.devops.service.config.TopicProperties;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceContext;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class KafkaTopicServiceImplIT {
    
    @Autowired
    private KafkaTopicService kafkaTopicService;
    
    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;
    
    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;
    
    @Autowired
    private KafkaTopicRepository kafkaTopicRepository;
    
    @Autowired
    private TopicProperties topicProperties;
    
    @Mock
    private KafkaHelperService kafkaHelperService;
    
    @PersistenceContext
    private EntityManager entityManager;
    
    @Before
    public void setup() {
        ReflectionTestUtils.setField(kafkaTopicService, "kafkaHelperService", kafkaHelperService);
    }
    
    @Test
    public void testCreate() {
        KafkaTopicDetailDTO kafkaTopicDetailDTO = createKafkaTopicDetail();
        
        KafkaTopicDetailDTO createdKafkaTopicDetailDTO = kafkaTopicService.create(kafkaTopicDetailDTO);
        
        Assertions.assertThat(createdKafkaTopicDetailDTO.getId()).isNotNull();
        Assertions.assertThat(createdKafkaTopicDetailDTO.getKafkaClusterId()).isEqualTo(kafkaTopicDetailDTO.getKafkaClusterId());
        Assertions.assertThat(createdKafkaTopicDetailDTO.getDataSystemResourceId()).isEqualTo(kafkaTopicDetailDTO.getDataSystemResourceId());
    }
    
    @Test
    public void testBatchCreate() {
        List<KafkaTopicDetailDTO> kafkaTopicDetailDTOs = createKafkaTopicDetailLimit3();
        
        List<KafkaTopicDetailDTO> createdKafkaTopicDetailDTOs = kafkaTopicService.batchCreate(kafkaTopicDetailDTOs);
        
        for (int i = 0; i < kafkaTopicDetailDTOs.size(); i++) {
            Assertions.assertThat(createdKafkaTopicDetailDTOs.get(i).getId()).isNotNull();
            Assertions.assertThat(createdKafkaTopicDetailDTOs.get(i).getKafkaClusterId()).isEqualTo(kafkaTopicDetailDTOs.get(i).getKafkaClusterId());
            Assertions.assertThat(createdKafkaTopicDetailDTOs.get(i).getDataSystemResourceId()).isEqualTo(kafkaTopicDetailDTOs.get(i).getDataSystemResourceId());
        }
        
        Assertions.assertThat(createdKafkaTopicDetailDTOs.size()).isEqualTo(3);
    }
    
    @Test
    public void testPagedQuery() {
        List<KafkaTopicDetailDTO> kafkaTopicDetailDTOs = createKafkaTopicDetailLimit3();
        kafkaTopicService.batchCreate(kafkaTopicDetailDTOs);
        
        KafkaTopicQuery kafkaTopicQuery = new KafkaTopicQuery();
        kafkaTopicQuery.setName(kafkaTopicDetailDTOs.get(0).getName());
        kafkaTopicQuery.setPageSize(99);
        kafkaTopicQuery.setCurrent(1);
        Page<KafkaTopicDTO> pageResult = kafkaTopicService.pagedQuery(kafkaTopicQuery);
        
        Assertions.assertThat(pageResult.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(pageResult.getContent().size()).isEqualTo(1);
    }
    
    @Test
    public void testQuery() {
        List<KafkaTopicDetailDTO> kafkaTopicDetailDTOs = createKafkaTopicDetailLimit3();
        kafkaTopicService.batchCreate(kafkaTopicDetailDTOs);
        
        KafkaTopicQuery kafkaTopicQuery = new KafkaTopicQuery();
        kafkaTopicQuery.setName(kafkaTopicDetailDTOs.get(0).getName());
        List<KafkaTopicDTO> kafkaTopics = kafkaTopicService.query(kafkaTopicQuery);
        
        Assertions.assertThat(kafkaTopics.size()).isEqualTo(1);
    }
    
    @Test
    public void testGetById() {
        KafkaTopicDetailDTO createdKafkaTopic = kafkaTopicService.create(createKafkaTopicDetail());
        
        Assertions.assertThat(kafkaTopicService.getById(createdKafkaTopic.getId()).getId())
                .isEqualTo(createdKafkaTopic.getId());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetByIdShouldThrowExceptionWhenNotFound() {
        kafkaTopicService.getById(-99L);
    }
    
    @Test
    public void testCreateDataCollectionTopicIfAbsent() {
        DataSystemResourceDO table = saveJdbcTable();
        KafkaClusterDO kafkaCluster = saveACDCKafkaCluster();
        
        KafkaTopicDetailDTO createdKafkaTopic = kafkaTopicService.createDataCollectionTopicIfAbsent(table.getId(), kafkaCluster.getId(), "acdc_test_topic");
        kafkaTopicService.createDataCollectionTopicIfAbsent(table.getId(), kafkaCluster.getId(), "acdc_test_topic");
        
        Assertions.assertThat(createdKafkaTopic.getId()).isNotNull();
        Assertions.assertThat(createdKafkaTopic.getKafkaClusterId()).isEqualTo(kafkaCluster.getId());
        Assertions.assertThat(createdKafkaTopic.getDataSystemResourceId()).isEqualTo(table.getId());
        Mockito.verify(kafkaHelperService, Mockito.times(1))
                .createTopic(eq("acdc_test_topic"),
                        eq(topicProperties.getDataCollection().getPartitions()),
                        eq(topicProperties.getDataCollection().getReplicationFactor()),
                        eq(topicProperties.getDataCollection().getConfigs()),
                        any(HashMap.class));
    }
    
    @Test
    public void testGetKafkaTopicByDataSystemResourceId() {
        DataSystemResourceDO table = saveJdbcTable();
        KafkaClusterDO kafkaCluster = saveACDCKafkaCluster();
        
        KafkaTopicDetailDTO kafkaTopicDetailDTO = new KafkaTopicDetailDTO()
                .setName("test_topic")
                .setKafkaClusterId(kafkaCluster.getId())
                .setDataSystemResourceId(table.getId());
        
        KafkaTopicDetailDTO createdKafkaTopic = kafkaTopicService.create(kafkaTopicDetailDTO);
        entityManager.clear();
        KafkaTopicDO foundKafkaTopic = kafkaTopicRepository.findByDataSystemResourceId(table.getId()).get();
        Assertions.assertThat(foundKafkaTopic.getId()).isEqualTo(createdKafkaTopic.getId());
    }
    
    @Test
    public void testGetKafkaTopicByDataSystemResourceIdWhenNotFound() {
        Assertions.assertThat(kafkaTopicRepository.findByDataSystemResourceId(-99L).isPresent()).isEqualTo(false);
    }
    
    @Test
    public void testCreateTICDCKafkaTopicIfAbsent() {
        DataSystemResourceDO database = saveJdbcDatabase();
        KafkaClusterDO ticdcKafkaCluster = saveTICDCKafkaCluster();
        
        KafkaTopicDetailDTO createdKafkaTopic = kafkaTopicService.createTICDCTopicIfAbsent("ticdc_test_topic", ticdcKafkaCluster.getId(), database.getId());
        kafkaTopicService.createTICDCTopicIfAbsent("ticdc_test_topic", ticdcKafkaCluster.getId(), database.getId());
        
        Assertions.assertThat(createdKafkaTopic.getId()).isNotNull();
        Assertions.assertThat(createdKafkaTopic.getKafkaClusterId()).isEqualTo(ticdcKafkaCluster.getId());
        Assertions.assertThat(createdKafkaTopic.getDataSystemResourceId()).isEqualTo(database.getId());
        
        Mockito.verify(kafkaHelperService, Mockito.times(1))
                .createTopic(eq("ticdc_test_topic"),
                        eq(topicProperties.getTicdc().getPartitions()),
                        eq(topicProperties.getTicdc().getReplicationFactor()),
                        eq(topicProperties.getTicdc().getConfigs()),
                        any(HashMap.class));
    }
    
    private KafkaTopicDetailDTO createKafkaTopicDetail() {
        DataSystemResourceDO tableDO = saveJdbcTable();
        KafkaClusterDO kafkaClusterDO = saveACDCKafkaCluster();
        return new KafkaTopicDetailDTO()
                .setKafkaClusterId(kafkaClusterDO.getId())
                .setDataSystemResourceId(tableDO.getId())
                .setName("test_kafka_topic");
    }
    
    private List<KafkaTopicDetailDTO> createKafkaTopicDetailLimit3() {
        DataSystemResourceDO tableDO1 = saveJdbcTable();
        DataSystemResourceDO tableDO2 = saveJdbcTable();
        DataSystemResourceDO tableDO3 = saveJdbcTable();
        KafkaClusterDO kafkaClusterDO = saveACDCKafkaCluster();
        return Lists.newArrayList(
                new KafkaTopicDetailDTO()
                        .setKafkaClusterId(kafkaClusterDO.getId())
                        .setDataSystemResourceId(tableDO1.getId())
                        .setName("test_kafka_topic1"),
                new KafkaTopicDetailDTO()
                        .setKafkaClusterId(kafkaClusterDO.getId())
                        .setDataSystemResourceId(tableDO2.getId())
                        .setName("test_kafka_topic2"),
                new KafkaTopicDetailDTO()
                        .setKafkaClusterId(kafkaClusterDO.getId())
                        .setDataSystemResourceId(tableDO3.getId())
                        .setName("test_kafka_topic3")
        );
    }
    
    private DataSystemResourceDO saveDataSystemResource(
            final String name,
            final DataSystemType dataSystemType,
            final DataSystemResourceType dataSystemResourceType,
            final DataSystemResourceDO parentResource
    
    ) {
        return dataSystemResourceRepository.save(
                new DataSystemResourceDO()
                        .setName(name)
                        .setDataSystemType(dataSystemType)
                        .setResourceType(dataSystemResourceType)
                        .setParentResource(parentResource)
        );
    }
    
    private DataSystemResourceDO saveJdbcTable() {
        DataSystemResourceDO database = saveDataSystemResource("db1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_DATABASE, null);
        DataSystemResourceDO table = saveDataSystemResource("tb1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, database);
        return table;
    }
    
    private DataSystemResourceDO saveJdbcDatabase() {
        DataSystemResourceDO database = saveDataSystemResource("db1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_DATABASE, null);
        return database;
    }
    
    private KafkaClusterDO saveACDCKafkaCluster() {
        return kafkaClusterRepository.save(new KafkaClusterDO()
                .setId(1L)
                .setName("test_kafka_cluster_ACDC")
                .setClusterType(KafkaClusterType.INNER)
                .setVersion("2.6")
                .setSecurityConfiguration("{}")
                .setBootstrapServers("localhost:9092")
        );
    }
    
    private KafkaClusterDO saveTICDCKafkaCluster() {
        return kafkaClusterRepository.save(new KafkaClusterDO()
                .setId(1L)
                .setName("test_kafka_cluster_TICDC")
                .setVersion("2.6")
                .setSecurityConfiguration("{}")
                .setClusterType(KafkaClusterType.TICDC)
                .setBootstrapServers("localhost:9092")
        );
    }
}
