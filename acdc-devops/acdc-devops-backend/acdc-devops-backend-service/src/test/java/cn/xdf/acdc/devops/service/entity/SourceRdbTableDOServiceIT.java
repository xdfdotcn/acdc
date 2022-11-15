package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class SourceRdbTableDOServiceIT {

    @Autowired
    private SourceRdbTableService sourceRdbTableService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private ConnectClusterService connectClusterService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private ConnectorDataExtensionService connectorDataExtensionService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        SourceRdbTableDO sourceRdbTableSaveResult = sourceRdbTableService.save(sourceRdbTable);
        Assertions.assertThat(sourceRdbTableSaveResult).isEqualTo(sourceRdbTable);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        SourceRdbTableDO saveResult1 = sourceRdbTableService.save(sourceRdbTable);

        saveResult1.setMaskHashAlgorithmSalt("test");
        SourceRdbTableDO saveResult2 = sourceRdbTableService.save(saveResult1);

        Assertions.assertThat(saveResult2.getMaskHashAlgorithmSalt()).isEqualTo("test");
        Assertions.assertThat(sourceRdbTableService.findAll().size()).isEqualTo(1);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        SourceRdbTableDO sourceRdbTableSaveResult = sourceRdbTableService.save(sourceRdbTable);
        Assertions.assertThat(sourceRdbTableService.findById(sourceRdbTableSaveResult.getId()).isPresent()).isEqualTo(true);
        Assertions.assertThat(sourceRdbTableService.findById(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> sourceRdbTableService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByConnectorIdShouldSuccess() {
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        sourceRdbTableService.save(sourceRdbTable);
//        Assertions.assertThat(sourceRdbTableService.findByConnectorId(sourceRdbTable.getConnector().getId()).isPresent()).isEqualTo(true);
//        Assertions.assertThat(sourceRdbTableService.findByConnectorId(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByConnectorIdShouldGetEmptyWhenGivenIllegalParameter() {
//        Assertions.assertThat(sourceRdbTableService.findByConnectorId(null).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByRdbTableIdShouldSuccess() {
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        sourceRdbTableService.save(sourceRdbTable);
        Assertions.assertThat(sourceRdbTableService.findByRdbTableId(sourceRdbTable.getRdbTable().getId()).isPresent()).isEqualTo(true);
//        Assertions.assertThat(sourceRdbTableService.findByConnectorId(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByRdbTableIdShouldGetEmptyWhenGivenIllegalParameter() {
        Assertions.assertThat(sourceRdbTableService.findByRdbTableId(null).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByKafkaTopicIdShouldSuccess() {
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        sourceRdbTableService.save(sourceRdbTable);
        Assertions.assertThat(sourceRdbTableService.findByKafkaTopicId(sourceRdbTable.getKafkaTopic().getId()).isPresent()).isEqualTo(true);
//        Assertions.assertThat(sourceRdbTableService.findByConnectorId(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByKafkaTopicIdShouldGetEmptyWhenGivenIllegalParameter() {
        Assertions.assertThat(sourceRdbTableService.findByKafkaTopicId(null).isPresent()).isEqualTo(false);
    }

    @Test
    public void testQueryByRdbTableIdList() {
        List<Long> saveIdList = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            RdbTableDO rdbTable = RdbTableDO.builder().name("test" + i).build();
            RdbTableDO saveRdbTable = rdbTableService.save(rdbTable);
            saveIdList.add(saveRdbTable.getId());
            SourceRdbTableDO sourceRdbTable = new SourceRdbTableDO();
            sourceRdbTable.setExcludedColumns("name,age,email");
            sourceRdbTable.setMaskedColumns("tel,password");
            sourceRdbTable.setMaskHashAlgorithmSalt("^*&");
            sourceRdbTable.setRdbTable(saveRdbTable);
            sourceRdbTableService.save(sourceRdbTable);
        }

        List<SourceRdbTableDO> findSourceRdbTables = sourceRdbTableService.queryByRdbTableIdList(saveIdList);
        Assertions.assertThat(findSourceRdbTables.size()).isEqualTo(3);

        Set<Long> rdbTableIds = findSourceRdbTables.stream().map(it -> it.getRdbTable().getId()).collect(Collectors.toSet());
        Assertions.assertThat(rdbTableIds.size()).isEqualTo(3);
    }

    @Test
    public void testCascade() {
        List<ConnectorDataExtensionDO> connectorDataExtensionList = connectorDataExtensionService
                .saveAll(createConnectorDataExtensionList());
        SourceRdbTableDO sourceRdbTable = createSourceRdbTable();
        sourceRdbTable.setConnectorDataExtensions(connectorDataExtensionList.stream().collect(Collectors.toSet()));
        SourceRdbTableDO sourceRdbTableSaveResult = sourceRdbTableService.save(sourceRdbTable);
        SourceRdbTableDO findSourceRdbTable = sourceRdbTableService.findById(sourceRdbTableSaveResult.getId()).get();

        connectorDataExtensionList.stream().sorted(Comparator.comparing(ConnectorDataExtensionDO::getId));

        List<ConnectorDataExtensionDO> findConnectorDataExtensionList = findSourceRdbTable.getConnectorDataExtensions().stream().sorted(Comparator.comparing(ConnectorDataExtensionDO::getId))
                .collect(Collectors.toList());

        Assertions.assertThat(findSourceRdbTable.getConnectorDataExtensions().size()).isEqualTo(connectorDataExtensionList.size());

        for (int i = 0; i < connectorDataExtensionList.size(); i++) {
            Assertions.assertThat(connectorDataExtensionList.get(i).getId()).isEqualTo(findConnectorDataExtensionList.get(i).getId());
        }
    }

    private SourceRdbTableDO createSourceRdbTable() {
        ConnectorDO connector = new ConnectorDO();
        connector.setName("test");
        ConnectorDO connectorSaveResult = connectorService.save(connector);

        RdbTableDO rdbTable = new RdbTableDO();
        rdbTable.setName("test");
        RdbTableDO rdbTableSaveResult = rdbTableService.save(rdbTable);

        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setName("t1");
        kafkaCluster.setVersion("kafka-v1.0");
        kafkaCluster.setClusterType(KafkaClusterType.INNER);
        kafkaCluster.setBootstrapServers("node-1,node-2");
        kafkaCluster.setSecurityConfiguration("sec");
        kafkaCluster.setDescription("desc");
        KafkaClusterDO kafkaClusterSaveResult = kafkaClusterService.save(kafkaCluster);

        KafkaTopicDO kafkaTopic = new KafkaTopicDO();
        kafkaTopic.setName("test");
        kafkaTopic.setKafkaCluster(kafkaClusterSaveResult);
        KafkaTopicDO kafkaTopicSaveResult = kafkaTopicService.save(kafkaTopic);

        SourceRdbTableDO sourceRdbTable = new SourceRdbTableDO();
        sourceRdbTable.setExcludedColumns("name,age,email");
        sourceRdbTable.setMaskedColumns("tel,password");
        sourceRdbTable.setMaskHashAlgorithmSalt("^*&");
        sourceRdbTable.setRdbTable(rdbTableSaveResult);
        sourceRdbTable.setConnector(connectorSaveResult);
        sourceRdbTable.setKafkaTopic(kafkaTopicSaveResult);
        return sourceRdbTable;
    }

    private List<ConnectorDataExtensionDO> createConnectorDataExtensionList() {
        List<ConnectorDataExtensionDO> connectorDataExtensionList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            ConnectorDataExtensionDO connectorDataExtension = new ConnectorDataExtensionDO();
            connectorDataExtension.setName("name" + i);
            connectorDataExtension.setValue("value" + i);
            connectorDataExtensionList.add(connectorDataExtension);
        }
        return connectorDataExtensionList;
    }
}
