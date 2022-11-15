package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class KafkaClusterServiceIT {

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setName("t1");
        kafkaCluster.setVersion("kafka-v1.0");
        kafkaCluster.setBootstrapServers("node-1,node-2");
        kafkaCluster.setClusterType(KafkaClusterType.TICDC);
        kafkaCluster.setSecurityConfiguration("sec");
        kafkaCluster.setDescription("desc");
        kafkaClusterService.save(kafkaCluster);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setName("t1");
        kafkaCluster.setVersion("kafka-v1.0");
        kafkaCluster.setBootstrapServers("node-1,node-2");
        kafkaCluster.setSecurityConfiguration("sec");
        kafkaCluster.setClusterType(KafkaClusterType.INNER);
        kafkaCluster.setDescription("desc");
        KafkaClusterDO saveResult1 = kafkaClusterService.save(kafkaCluster);
        saveResult1.setDescription("test2");
        KafkaClusterDO saveResult2 = kafkaClusterService.save(saveResult1);
        Assertions.assertThat(saveResult2.getDescription()).isEqualTo("test2");
        Assertions.assertThat(kafkaClusterService.findAll().size()).isEqualTo(1);
    }

    @Test(expected = DataIntegrityViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaClusterService.save(kafkaCluster);
    }

    @Test
    public void testSaveShouldThrowExceptionWhenGivenNull() {
        Throwable throwable = Assertions.catchThrowable(() -> kafkaClusterService.save(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setName("t1");
        kafkaCluster.setVersion("kafka-v1.0");
        kafkaCluster.setClusterType(KafkaClusterType.INNER);
        kafkaCluster.setBootstrapServers("node-1,node-2");
        kafkaCluster.setSecurityConfiguration("sec");
        kafkaCluster.setDescription("desc");
        KafkaClusterDO saveResult = kafkaClusterService.save(kafkaCluster);
        KafkaClusterDO findResult = kafkaClusterService.findById(saveResult.getId()).get();
        Assertions.assertThat(findResult).isEqualTo(saveResult);
        Assertions.assertThat(kafkaClusterService.findById(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenNullId() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setName("t1");
        kafkaCluster.setVersion("kafka-v1.0");
        kafkaCluster.setBootstrapServers("node-1,node-2");
        kafkaCluster.setClusterType(KafkaClusterType.INNER);
        kafkaCluster.setSecurityConfiguration("sec");
        kafkaCluster.setDescription("desc");
        kafkaClusterService.save(kafkaCluster);
        Throwable throwable = Assertions.catchThrowable(() -> kafkaClusterService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testFindByClusterType() {
        KafkaClusterDO kafkaClusterInner = new KafkaClusterDO();
        kafkaClusterInner.setName("t1");
        kafkaClusterInner.setVersion("kafka-v1.0");
        kafkaClusterInner.setBootstrapServers("node-1,node-2");
        kafkaClusterInner.setSecurityConfiguration("sec");
        kafkaClusterInner.setClusterType(KafkaClusterType.INNER);
        kafkaClusterInner.setDescription("desc");
        kafkaClusterService.save(kafkaClusterInner);

        KafkaClusterDO kafkaClusterTidb = new KafkaClusterDO();
        kafkaClusterTidb.setName("t2");
        kafkaClusterTidb.setVersion("kafka-v1.0");
        kafkaClusterTidb.setBootstrapServers("node-1,node-2");
        kafkaClusterTidb.setSecurityConfiguration("sec");

        kafkaClusterTidb.setClusterType(KafkaClusterType.TICDC);
        kafkaClusterTidb.setDescription("desc");
        kafkaClusterService.save(kafkaClusterTidb);

        Optional<KafkaClusterDO> tidbOpt = kafkaClusterService.findTicdcKafkaCluster();
        Assertions.assertThat(tidbOpt.get().getClusterType()).isEqualTo(KafkaClusterType.TICDC);

        Optional<KafkaClusterDO> innerOpt = kafkaClusterService.findInnerKafkaCluster();
        Assertions.assertThat(innerOpt.get().getClusterType()).isEqualTo(KafkaClusterType.INNER);
    }
}
