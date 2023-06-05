package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourceQuery;
import com.google.common.collect.Sets;
import javassist.NotFoundException;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class DataSystemResourceRepositoryT {
    
    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;
    
    @Autowired
    private DataSystemResourceConfigurationRepository dataSystemResourceConfigurationRepository;
    
    @Autowired
    private KafkaTopicRepository kafkaTopicRepository;
    
    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;
    
    @PersistenceContext
    private EntityManager entityManager;
    
    @Test
    public void testOneToManyCascadeInsertShouldPersistModelsAndRelations() throws NotFoundException {
        DataSystemResourceConfigurationDO config1 = new DataSystemResourceConfigurationDO().setName("key_1").setValue("value_1");
        DataSystemResourceConfigurationDO config2 = new DataSystemResourceConfigurationDO().setName("key_2").setValue("value_2");
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = Sets.newHashSet(config1, config2);
        
        DataSystemResourceDO dataSystem1 =
                new DataSystemResourceDO()
                        .setDataSystemResourceConfigurations(dataSystemResourceConfigurations)
                        .setName("data_system_1")
                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .setDataSystemType(DataSystemType.MYSQL);
        
        config1.setDataSystemResource(dataSystem1);
        config2.setDataSystemResource(dataSystem1);
        
        DataSystemResourceDO saveResult = dataSystemResourceRepository.save(dataSystem1);
        entityManager.flush();
        entityManager.clear();
        DataSystemResourceDO findResult = dataSystemResourceRepository.findById(saveResult.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        
        Set<DataSystemResourceConfigurationDO> saveConfigs = saveResult.getDataSystemResourceConfigurations();
        Set<Long> configIds = saveConfigs.stream().map(DataSystemResourceConfigurationDO::getId).collect(Collectors.toSet());
        entityManager.flush();
        entityManager.clear();
        List<DataSystemResourceConfigurationDO> findConfigs = dataSystemResourceConfigurationRepository.findAllById(configIds);
        
        Assertions.assertThat(findConfigs.size()).isEqualTo(2);
        Assertions.assertThat(findConfigs.get(0).getName()).isEqualTo("key_1");
        Assertions.assertThat(findConfigs.get(1).getName()).isEqualTo("key_2");
    }
    
    @Test
    public void testOneToManyCascadeUpdateShouldUpdateAllModels() throws NotFoundException {
        DataSystemResourceConfigurationDO config3 = new DataSystemResourceConfigurationDO().setName("key_3").setValue("value_3");
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = Sets.newHashSet(config3);
        
        DataSystemResourceDO dataSystem3 =
                new DataSystemResourceDO()
                        .setDataSystemResourceConfigurations(dataSystemResourceConfigurations)
                        .setName("data_system_3")
                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .setDataSystemType(DataSystemType.MYSQL);
        
        config3.setDataSystemResource(dataSystem3);
        DataSystemResourceDO saveResult = dataSystemResourceRepository.save(dataSystem3);
        
        DataSystemResourceDO findResult = dataSystemResourceRepository.findById(saveResult.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        
        findResult.setName("data_system_3_update");
        DataSystemResourceConfigurationDO findConfigResult = findResult.getDataSystemResourceConfigurations().stream().findFirst()
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        findConfigResult.setName("key_3_update");
        
        dataSystemResourceRepository.save(dataSystem3);
        
        entityManager.flush();
        entityManager.clear();
        DataSystemResourceDO findUpdatedResult = dataSystemResourceRepository.findById(saveResult.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        
        Assertions.assertThat(findUpdatedResult.getName()).isEqualTo(findResult.getName());
        
        DataSystemResourceConfigurationDO findUpdatedConfigResult = dataSystemResourceConfigurationRepository.findById(findConfigResult.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findUpdatedConfigResult.getName()).isEqualTo(findConfigResult.getName());
    }
    
    @Test
    public void testOneToManyCascadeDeleteShouldDeleteAllModels() throws NotFoundException {
        DataSystemResourceConfigurationDO config4 = new DataSystemResourceConfigurationDO().setName("key_4").setValue("value_4");
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = Sets.newHashSet(config4);
        
        DataSystemResourceDO dataSystem4 =
                new DataSystemResourceDO()
                        .setDataSystemResourceConfigurations(dataSystemResourceConfigurations)
                        .setName("data_system_4")
                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .setDataSystemType(DataSystemType.MYSQL);
        config4.setDataSystemResource(dataSystem4);
        
        DataSystemResourceDO saveResult = dataSystemResourceRepository.save(dataSystem4);
        
        DataSystemResourceDO findResult = dataSystemResourceRepository.findById(saveResult.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        final DataSystemResourceConfigurationDO findConfigResult = findResult.getDataSystemResourceConfigurations().stream().findFirst()
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        
        dataSystemResourceRepository.deleteById(saveResult.getId());
        
        entityManager.flush();
        entityManager.clear();
        Optional<DataSystemResourceDO> findUpdatedResult = dataSystemResourceRepository.findById(saveResult.getId());
        Assertions.assertThat(findUpdatedResult.isPresent()).isFalse();
        
        Optional<DataSystemResourceConfigurationDO> findUpdatedConfigResult = dataSystemResourceConfigurationRepository.findById(findConfigResult.getId());
        Assertions.assertThat(findUpdatedConfigResult.isPresent()).isFalse();
    }
    
    @Test
    public void testOneToOneForwardCascadeInsertShouldPersistModelsAndRelations() throws NotFoundException {
        KafkaClusterDO kafkaClusterDO = new KafkaClusterDO().setName("kafka-cluster").setVersion("2.6")
                .setBootstrapServers("localhost:9092");
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);
        
        KafkaTopicDO topic1 = new KafkaTopicDO().setName("topic_1").setKafkaCluster(kafkaClusterDO);
        
        DataSystemResourceDO dataSystem1 =
                new DataSystemResourceDO()
                        .setName("data_system_11")
                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .setDataSystemType(DataSystemType.MYSQL);
        DataSystemResourceDO savedDataSystemResource = dataSystemResourceRepository.save(dataSystem1);
        
        topic1.setDataSystemResource(savedDataSystemResource);
        KafkaTopicDO savedKafkaTopic = kafkaTopicRepository.save(topic1);
        entityManager.flush();
        entityManager.clear();
        KafkaTopicDO findKafkaTopic = kafkaTopicRepository.findById(savedKafkaTopic.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findKafkaTopic.getDataSystemResource()).isEqualTo(savedDataSystemResource);
        
        DataSystemResourceDO findDataSystemResource = dataSystemResourceRepository.findById(savedDataSystemResource.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findDataSystemResource.getKafkaTopic()).isEqualTo(savedKafkaTopic);
    }
    
    @Test
    public void testOneToOneBackWardCascadeInsertShouldNotPersistRelations() throws NotFoundException {
        KafkaClusterDO kafkaClusterDO = new KafkaClusterDO().setName("kafka-cluster").setVersion("2.6")
                .setBootstrapServers("localhost:9092");
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);
        
        KafkaTopicDO topic1 = new KafkaTopicDO().setName("topic_1").setKafkaCluster(kafkaClusterDO);
        KafkaTopicDO savedKafkaTopic = kafkaTopicRepository.save(topic1);
        
        DataSystemResourceDO dataSystem1 =
                new DataSystemResourceDO()
                        .setKafkaTopic(savedKafkaTopic)
                        .setName("data_system_11")
                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .setDataSystemType(DataSystemType.MYSQL);
        DataSystemResourceDO savedDataSystemResource = dataSystemResourceRepository.save(dataSystem1);
        
        entityManager.flush();
        entityManager.clear();
        
        DataSystemResourceDO findDataSystemResource = dataSystemResourceRepository.findById(savedDataSystemResource.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findDataSystemResource.getKafkaTopic()).isNull();
    }
    
    @Test
    public void testManyToOneCascadeInsertShouldPersistRelationsWithIdSet() throws NotFoundException {
        KafkaClusterDO kafkaClusterDO = new KafkaClusterDO()
                .setName("kafka-cluster")
                .setVersion("2.6")
                .setBootstrapServers("localhost:9092");
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);
        
        KafkaTopicDO topic1 = new KafkaTopicDO().setName("topic_1").setKafkaCluster(new KafkaClusterDO(kafkaClusterDO.getId()));
        
        KafkaTopicDO savedKafkaTopic = kafkaTopicRepository.save(topic1);
        entityManager.flush();
        entityManager.clear();
        KafkaTopicDO findKafkaTopic = kafkaTopicRepository.findById(savedKafkaTopic.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findKafkaTopic.getKafkaCluster()).isEqualTo(kafkaClusterDO);
    }
    
    @Test
    public void testOneToOneCascadeDeleteShouldPersistModelsAndRelations() throws NotFoundException {
        KafkaClusterDO kafkaClusterDO = new KafkaClusterDO().setName("kafka-cluster").setVersion("2.6")
                .setBootstrapServers("localhost:9092");
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);
        
        KafkaTopicDO topic1 = new KafkaTopicDO().setName("topic_1").setKafkaCluster(new KafkaClusterDO(kafkaClusterDO.getId()));
        
        DataSystemResourceDO dataSystem1 =
                new DataSystemResourceDO()
                        .setName("data_system_11")
                        .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .setDataSystemType(DataSystemType.MYSQL);
        DataSystemResourceDO savedDataSystemResource = dataSystemResourceRepository.save(dataSystem1);
        
        topic1.setDataSystemResource(savedDataSystemResource);
        KafkaTopicDO savedKafkaTopic = kafkaTopicRepository.save(topic1);
        kafkaTopicRepository.deleteById(savedKafkaTopic.getId());
        entityManager.flush();
        entityManager.clear();
        Optional<KafkaTopicDO> findKafkaTopic = kafkaTopicRepository.findById(savedKafkaTopic.getId());
        Assertions.assertThat(findKafkaTopic.isPresent()).isFalse();
        
        DataSystemResourceDO findDataSystemResource = dataSystemResourceRepository.findById(savedDataSystemResource.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findDataSystemResource.getKafkaTopic()).isNull();
    }
    
    @Test
    public void testQueryShouldReturnChild() {
        // create do
        DataSystemResourceDO parent = new DataSystemResourceDO()
                .setName("parent")
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemType(DataSystemType.MYSQL);
        
        DataSystemResourceConfigurationDO dataSystemResourceConfigurationDO = new DataSystemResourceConfigurationDO()
                .setName("configuration_name")
                .setValue("configuration_value");
        
        DataSystemResourceDO child = new DataSystemResourceDO()
                .setName("child")
                .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
                .setDataSystemType(DataSystemType.MYSQL)
                .setParentResource(parent)
                .setDataSystemResourceConfigurations(Sets.newHashSet(dataSystemResourceConfigurationDO));
        
        dataSystemResourceConfigurationDO.setDataSystemResource(child);
        
        dataSystemResourceRepository.save(parent);
        dataSystemResourceRepository.save(child);
        
        // do query
        Map<String, String> configurationParameters = new HashMap<>();
        configurationParameters.put(dataSystemResourceConfigurationDO.getName(), dataSystemResourceConfigurationDO.getValue());
        
        DataSystemResourceQuery dataSystemResourceQuery = new DataSystemResourceQuery();
        dataSystemResourceQuery.setParentResourceId(parent.getId());
        dataSystemResourceQuery.setResourceConfigurations(configurationParameters);
        
        List<DataSystemResourceDO> dataSystemResources = dataSystemResourceRepository.query(dataSystemResourceQuery);
        
        Assertions.assertThat(dataSystemResources.size()).isEqualTo(1);
        Assertions.assertThat(dataSystemResources.get(0).getId()).isEqualTo(child.getId());
    }
    
    @Test
    public void testOneToManyCascadeShouldReturnNotEmptyChildrenResource() {
        // create do
        DataSystemResourceDO parent = new DataSystemResourceDO()
                .setName("parent")
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemType(DataSystemType.MYSQL);
        
        DataSystemResourceConfigurationDO dataSystemResourceConfigurationDO = new DataSystemResourceConfigurationDO()
                .setName("configuration_name")
                .setValue("configuration_value");
        
        DataSystemResourceDO child = new DataSystemResourceDO()
                .setName("child")
                .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
                .setDataSystemType(DataSystemType.MYSQL)
                .setParentResource(parent)
                .setDataSystemResourceConfigurations(Sets.newHashSet(dataSystemResourceConfigurationDO));
        
        dataSystemResourceConfigurationDO.setDataSystemResource(child);
        
        dataSystemResourceRepository.save(parent);
        dataSystemResourceRepository.save(child);
        
        entityManager.flush();
        entityManager.clear();
        
        DataSystemResourceDO gottenParent = dataSystemResourceRepository.getOne(parent.getId());
        Assertions.assertThat(gottenParent.getChildrenResources().size()).isEqualTo(1);
    }
}
