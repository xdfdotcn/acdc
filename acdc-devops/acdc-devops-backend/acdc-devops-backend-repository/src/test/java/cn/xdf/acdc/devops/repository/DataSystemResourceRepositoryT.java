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
        DataSystemResourceConfigurationDO config1 = DataSystemResourceConfigurationDO.builder().name("key_1").value("value_1").build();
        DataSystemResourceConfigurationDO config2 = DataSystemResourceConfigurationDO.builder().name("key_2").value("value_2").build();
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = Sets.newHashSet(config1, config2);

        DataSystemResourceDO dataSystem1 =
                DataSystemResourceDO.builder()
                        .dataSystemResourceConfigurations(dataSystemResourceConfigurations)
                        .name("data_system_1").resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .dataSystemType(DataSystemType.MYSQL).build();

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
        DataSystemResourceConfigurationDO config3 = DataSystemResourceConfigurationDO.builder().name("key_3").value("value_3").build();
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = Sets.newHashSet(config3);

        DataSystemResourceDO dataSystem3 =
                DataSystemResourceDO.builder()
                        .dataSystemResourceConfigurations(dataSystemResourceConfigurations)
                        .name("data_system_3")
                        .resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .dataSystemType(DataSystemType.MYSQL)
                        .build();

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
        DataSystemResourceConfigurationDO config4 = DataSystemResourceConfigurationDO.builder().name("key_4").value("value_4").build();
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = Sets.newHashSet(config4);

        DataSystemResourceDO dataSystem4 =
                DataSystemResourceDO.builder()
                        .dataSystemResourceConfigurations(dataSystemResourceConfigurations)
                        .name("data_system_4").resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .dataSystemType(DataSystemType.MYSQL).build();
        config4.setDataSystemResource(dataSystem4);

        DataSystemResourceDO saveResult = dataSystemResourceRepository.save(dataSystem4);

        DataSystemResourceDO findResult = dataSystemResourceRepository.findById(saveResult.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        DataSystemResourceConfigurationDO findConfigResult = findResult.getDataSystemResourceConfigurations().stream().findFirst()
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
        KafkaClusterDO kafkaClusterDO = KafkaClusterDO.builder().name("kafka-cluster").version("2.6")
                .bootstrapServers("localhost:9092").build();
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);

        KafkaTopicDO topic1 = KafkaTopicDO.builder().name("topic_1").kafkaCluster(kafkaClusterDO).build();

        DataSystemResourceDO dataSystem1 =
                DataSystemResourceDO.builder()
                        .name("data_system_11").resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .dataSystemType(DataSystemType.MYSQL).build();
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
        KafkaClusterDO kafkaClusterDO = KafkaClusterDO.builder().name("kafka-cluster").version("2.6")
                .bootstrapServers("localhost:9092").build();
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);

        KafkaTopicDO topic1 = KafkaTopicDO.builder().name("topic_1").kafkaCluster(kafkaClusterDO).build();
        KafkaTopicDO savedKafkaTopic = kafkaTopicRepository.save(topic1);

        DataSystemResourceDO dataSystem1 =
                DataSystemResourceDO.builder().kafkaTopic(savedKafkaTopic)
                        .name("data_system_11")
                        .resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .dataSystemType(DataSystemType.MYSQL)
                        .build();
        DataSystemResourceDO savedDataSystemResource = dataSystemResourceRepository.save(dataSystem1);

        entityManager.flush();
        entityManager.clear();

        DataSystemResourceDO findDataSystemResource = dataSystemResourceRepository.findById(savedDataSystemResource.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findDataSystemResource.getKafkaTopic()).isNull();
    }

    @Test
    public void testManyToOneCascadeInsertShouldPersistRelationsWithIdSet() throws NotFoundException {
        KafkaClusterDO kafkaClusterDO = KafkaClusterDO.builder().name("kafka-cluster").version("2.6")
                .bootstrapServers("localhost:9092").build();
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);

        KafkaTopicDO topic1 = KafkaTopicDO.builder().name("topic_1").kafkaCluster(new KafkaClusterDO(kafkaClusterDO.getId())).build();

        KafkaTopicDO savedKafkaTopic = kafkaTopicRepository.save(topic1);
        entityManager.flush();
        entityManager.clear();
        KafkaTopicDO findKafkaTopic = kafkaTopicRepository.findById(savedKafkaTopic.getId())
                .orElseThrow(() -> new NotFoundException("could not found data system resource."));
        Assertions.assertThat(findKafkaTopic.getKafkaCluster()).isEqualTo(kafkaClusterDO);
    }

    @Test
    public void testOneToOneCascadeDeleteShouldPersistModelsAndRelations() throws NotFoundException {
        KafkaClusterDO kafkaClusterDO = KafkaClusterDO.builder().name("kafka-cluster").version("2.6")
                .bootstrapServers("localhost:9092").build();
        kafkaClusterDO = kafkaClusterRepository.save(kafkaClusterDO);

        KafkaTopicDO topic1 = KafkaTopicDO.builder().name("topic_1").kafkaCluster(new KafkaClusterDO(kafkaClusterDO.getId())).build();

        DataSystemResourceDO dataSystem1 =
                DataSystemResourceDO.builder()
                        .name("data_system_11").resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                        .dataSystemType(DataSystemType.MYSQL).build();
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
        DataSystemResourceDO parent = DataSystemResourceDO.builder()
                .name("parent")
                .resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .dataSystemType(DataSystemType.MYSQL)
                .build();

        DataSystemResourceConfigurationDO dataSystemResourceConfigurationDO = DataSystemResourceConfigurationDO.builder()
                .name("configuration_name")
                .value("configuration_value")
                .build();

        DataSystemResourceDO child = DataSystemResourceDO.builder()
                .name("child")
                .resourceType(DataSystemResourceType.MYSQL_DATABASE)
                .dataSystemType(DataSystemType.MYSQL)
                .parentResource(parent)
                .dataSystemResourceConfigurations(Sets.newHashSet(dataSystemResourceConfigurationDO))
                .build();

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
        DataSystemResourceDO parent = DataSystemResourceDO.builder()
                .name("parent")
                .resourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .dataSystemType(DataSystemType.MYSQL)
                .build();

        DataSystemResourceConfigurationDO dataSystemResourceConfigurationDO = DataSystemResourceConfigurationDO.builder()
                .name("configuration_name")
                .value("configuration_value")
                .build();

        DataSystemResourceDO child = DataSystemResourceDO.builder()
                .name("child")
                .resourceType(DataSystemResourceType.MYSQL_DATABASE)
                .dataSystemType(DataSystemType.MYSQL)
                .parentResource(parent)
                .dataSystemResourceConfigurations(Sets.newHashSet(dataSystemResourceConfigurationDO))
                .build();

        dataSystemResourceConfigurationDO.setDataSystemResource(child);

        dataSystemResourceRepository.save(parent);
        dataSystemResourceRepository.save(child);

        entityManager.flush();
        entityManager.clear();

        DataSystemResourceDO gottenParent = dataSystemResourceRepository.getOne(parent.getId());
        Assertions.assertThat(gottenParent.getChildrenResources().size()).isEqualTo(1);
    }
}
