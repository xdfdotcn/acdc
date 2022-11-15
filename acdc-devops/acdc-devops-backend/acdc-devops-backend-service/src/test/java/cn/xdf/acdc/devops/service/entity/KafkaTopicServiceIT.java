package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class KafkaTopicServiceIT {

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    private KafkaClusterDO kafkaClusterSaveResult;

    @Before
    public void init() {
        KafkaClusterDO kafkaCluster = new KafkaClusterDO();
        kafkaCluster.setName("t1");
        kafkaCluster.setVersion("kafka-v1.0");
        kafkaCluster.setBootstrapServers("node-1,node-2");
        kafkaCluster.setSecurityConfiguration("sec");
        kafkaCluster.setDescription("desc");
        kafkaCluster.setClusterType(KafkaClusterType.INNER);
        kafkaClusterSaveResult = kafkaClusterService.save(kafkaCluster);
    }

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        KafkaTopicDO kafkaTopic = createKafkaTopic();
        KafkaTopicDO kafkaTopicSaveResult = kafkaTopicService.save(kafkaTopic);
        kafkaTopic.setId(kafkaTopicSaveResult.getId());
        Assertions.assertThat(kafkaTopicSaveResult).isEqualTo(kafkaTopic);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        KafkaTopicDO kafkaTopic = createKafkaTopic();
        KafkaTopicDO saveResult1 = kafkaTopicService.save(kafkaTopic);
        saveResult1.setName("test2");
        KafkaTopicDO saveResult2 = kafkaTopicService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(kafkaTopicService.queryAll(new KafkaTopicDO()).size()).isEqualTo(1);
    }

    private KafkaTopicDO createKafkaTopic() {
        KafkaTopicDO kafkaTopic = new KafkaTopicDO();
        kafkaTopic.setName("test");
        kafkaTopic.setKafkaCluster(kafkaClusterSaveResult);
        return kafkaTopic;
    }

    @Test
    public void testSaveShouldThrowExceptionWhenFieldValidateFail() {
        KafkaTopicDO kafkaTopic = new KafkaTopicDO();
        kafkaTopic.setName(null);
        Throwable throwable = Assertions.catchThrowable(() -> kafkaTopicService.save(kafkaTopic));
        Assertions.assertThat(throwable).isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<KafkaTopicDO> rdbTableList = createKafkaTopicList();
        List<KafkaTopicDO> saveResultList = kafkaTopicService.saveAll(rdbTableList);

        Assertions.assertThat(saveResultList.size()).isEqualTo(rdbTableList.size());

        for (int i = 0; i < rdbTableList.size(); i++) {
            rdbTableList.get(i).setId(saveResultList.get(i).getId());
            Assertions.assertThat(saveResultList.get(i)).isEqualTo(rdbTableList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenAlreadyExist() {
        List<KafkaTopicDO> rdbTableList = createKafkaTopicList();
        List<KafkaTopicDO> saveResultList = kafkaTopicService.saveAll(rdbTableList);
        saveResultList.forEach(table -> table.setName("test_update"));
        kafkaTopicService.saveAll(saveResultList).forEach(topic -> {
            Assertions.assertThat(topic.getName()).isEqualTo("test_update");
        });
        Assertions.assertThat(kafkaTopicService.saveAll(saveResultList).size()).isEqualTo(rdbTableList.size());
    }

    @Test
    public void testSaveAllShouldDoNothingWhenGivenEmptyCollection() {
        kafkaTopicService.saveAll(Lists.newArrayList());
        Assertions.assertThat(kafkaTopicService.queryAll(new KafkaTopicDO()).size()).isEqualTo(0);
    }

    @Test
    public void testSaveAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> kafkaTopicService.saveAll(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<KafkaTopicDO> kafkaTopicList = createKafkaTopicListByCount(20);
        kafkaTopicService.saveAll(kafkaTopicList);
        KafkaTopicDO kafkaTopic = new KafkaTopicDO();
        kafkaTopic.setName("k1");
        List<KafkaTopicDO> queryResultList = kafkaTopicService.queryAll(kafkaTopic);
        Assertions.assertThat(queryResultList.size()).isEqualTo(10);
        queryResultList.forEach(r -> Assertions.assertThat(r.getName()).contains("k1"));

        queryResultList = kafkaTopicService.queryAll(new KafkaTopicDO());
        long p1Count = queryResultList.stream().filter(item -> item.getName().contains("k1")).count();
        long p2Count = queryResultList.stream().filter(item -> item.getName().contains("k2")).count();
        Assertions.assertThat(queryResultList.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test
    public void testQueryAllShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> kafkaTopicService.queryAll(null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testQueryShouldSuccess() {
        List<KafkaTopicDO> kafkaTopicList = createKafkaTopicList();
        kafkaTopicService.saveAll(kafkaTopicList);

        // 分页正常滚动
        KafkaTopicDO kafkaTopic = new KafkaTopicDO();
        kafkaTopic.setName("rdbTable");
        Page<KafkaTopicDO> page = kafkaTopicService.query(kafkaTopic, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = kafkaTopicService.query(kafkaTopic, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = kafkaTopicService.query(kafkaTopic, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = kafkaTopicService.query(kafkaTopic, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        kafkaTopic.setName("kk-not-exist");
        page = kafkaTopicService.query(kafkaTopic, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = kafkaTopicService.query(new KafkaTopicDO(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        kafkaTopic.setName("rdbTable");
        page = kafkaTopicService.query(kafkaTopic, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test
    public void testQueryShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> kafkaTopicService.query(null, null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);

        throwable = Assertions.catchThrowable(() -> kafkaTopicService.query(new KafkaTopicDO(), null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);

        throwable = Assertions.catchThrowable(() -> kafkaTopicService.query(new KafkaTopicDO(), createPageRequest(-999, -666)));
        Assertions.assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testFindByIdShouldSuccess() {
        KafkaTopicDO kafkaTopic = createKafkaTopic();
        KafkaTopicDO kafkaTopicSaveResult = kafkaTopicService.save(kafkaTopic);
        Assertions.assertThat(kafkaTopicService.findById(kafkaTopicSaveResult.getId()).isPresent()).isEqualTo(true);
        Assertions.assertThat(kafkaTopicService.findById(99L).isPresent()).isEqualTo(false);
    }

    @Test
    public void testFindByIdShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable = Assertions.catchThrowable(() -> kafkaTopicService.findById(null));
        Assertions.assertThat(throwable).isInstanceOf(InvalidDataAccessApiUsageException.class);
    }

    @Test
    public void testCascade() {
        KafkaTopicDO kafkaTopicSaveResult = kafkaTopicService.save(createKafkaTopic());
        KafkaTopicDO findKafkaTopic = kafkaTopicService.findById(kafkaTopicSaveResult.getId()).get();
        Assertions.assertThat(findKafkaTopic).isNotNull();
        Assertions.assertThat(findKafkaTopic.getKafkaCluster().getId()).isEqualTo(kafkaClusterSaveResult.getId());
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<KafkaTopicDO> createKafkaTopicListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<KafkaTopicDO> kafkaTopicList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String name = i % 2 == 0 ? "k1-" + i : "k2-" + i;
            KafkaTopicDO kafkaTopic = new KafkaTopicDO();
            kafkaTopic.setName(name);
            kafkaTopic.setKafkaCluster(kafkaClusterSaveResult);
            kafkaTopicList.add(kafkaTopic);

        }
        return kafkaTopicList;
    }

    private List<KafkaTopicDO> createKafkaTopicList() {
        List<KafkaTopicDO> kafkaTopicList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            KafkaTopicDO kafkaTopic = new KafkaTopicDO();
            kafkaTopic.setKafkaCluster(kafkaClusterSaveResult);
            kafkaTopic.setName("rdbTable" + i);
            kafkaTopicList.add(kafkaTopic);
        }
        return kafkaTopicList;
    }

}
