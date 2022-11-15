// CHECKSTYLE:OFF
package cn.xdf.acdc.devops.service.process.datasystem.kafka.impl;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaTopicProcessServiceImplTest {

    @MockBean
    private KafkaHelperService kafkaHelperService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private DataSystemMetadataService<KafkaClusterDO> dataSystemMetadataService;

    @Before
    public void init() {
        initKafkaClusterAndTopic();
    }

    @Test
    public void testShouldInsertToDatabase() {
        when(kafkaHelperService.listTopics(any())).thenReturn(Sets.newHashSet("mock-topic-1", "mock-topic-2", "mock-topic-3", "mock-topic-4"));
        dataSystemMetadataService.refreshMetadata();
        KafkaTopicDO kafkaTopicDO = buildQueryParam(1L);
        List<KafkaTopicDO> kafkaTopics = kafkaTopicService.queryAll(kafkaTopicDO);
        Assertions.assertThat(kafkaTopics.size()).isEqualTo(4);
    }

    @Test
    public void testShouldDeleteFromDatabase() {
        when(kafkaHelperService.listTopics(any())).thenReturn(Sets.newHashSet("mock-topic-1"));
        dataSystemMetadataService.refreshMetadata();
        List<KafkaTopicDO> kafkaTopics = kafkaTopicService.queryAll(buildQueryParam(1L));
        Assertions.assertThat(kafkaTopics.size()).isEqualTo(1);
        Assertions.assertThat(kafkaTopics.get(0).getName()).isEqualTo("mock-topic-1");
    }

    @Test
    public void testShouldDoNothing() {
        when(kafkaHelperService.listTopics(any())).thenReturn(Sets.newHashSet("mock-topic-1", "mock-topic-4"));
        dataSystemMetadataService.refreshMetadata();
        List<KafkaTopicDO> kafkaTopics = kafkaTopicService.queryAll(buildQueryParam(1L));
        Assertions.assertThat(kafkaTopics.size()).isEqualTo(2);
    }

    @Test
    public void testShouldNotRefreshForInnerCluster() {
        when(kafkaHelperService.listTopics(any())).thenReturn(Sets.newHashSet("mock-topic-1", "mock-topic-4"));
        dataSystemMetadataService.refreshMetadata();
        List<KafkaTopicDO> kafkaTopics = kafkaTopicService.queryAll(buildQueryParam(2L));
        Assertions.assertThat(kafkaTopics.size()).isEqualTo(0);
    }

    private void initKafkaClusterAndTopic() {
        KafkaClusterDO kafkaClusterOfUser = new KafkaClusterDO();
        kafkaClusterOfUser.setClusterType(KafkaClusterType.USER);
        kafkaClusterOfUser.setBootstrapServers("");
        kafkaClusterOfUser.setName("mock-test-kafka-user");
        kafkaClusterOfUser.setDescription("for test of user");
        kafkaClusterOfUser.setSecurityConfiguration("");
        kafkaClusterOfUser.setVersion("2.6.0");
        kafkaClusterService.save(kafkaClusterOfUser);

        KafkaClusterDO kafkaClusterOfInner = new KafkaClusterDO();
        kafkaClusterOfInner.setClusterType(KafkaClusterType.INNER);
        kafkaClusterOfInner.setBootstrapServers("");
        kafkaClusterOfInner.setName("mock-test-kafka-inner");
        kafkaClusterOfInner.setDescription("for test of inner");
        kafkaClusterOfInner.setSecurityConfiguration("");
        kafkaClusterOfInner.setVersion("2.6.0");
        kafkaClusterService.save(kafkaClusterOfInner);

        KafkaTopicDO topic = new KafkaTopicDO();
        topic.setKafkaCluster(kafkaClusterOfUser);
        topic.setName("mock-topic-1");

        KafkaTopicDO topic1 = new KafkaTopicDO();
        topic1.setKafkaCluster(kafkaClusterOfUser);
        topic1.setName("mock-topic-4");

        kafkaTopicService.saveAll(Lists.newArrayList(topic, topic1));

    }

    private KafkaTopicDO buildQueryParam(final Long id) {
        return KafkaTopicDO.builder()
                .kafkaCluster(KafkaClusterDO.builder().id(id).build())
                .deleted(Boolean.FALSE).build();
    }

    @After
    public void reset() {
        Mockito.reset(kafkaHelperService);
    }
}
