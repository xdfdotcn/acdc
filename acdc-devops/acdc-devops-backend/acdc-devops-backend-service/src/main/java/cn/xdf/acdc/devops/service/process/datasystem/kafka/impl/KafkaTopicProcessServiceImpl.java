package cn.xdf.acdc.devops.service.process.datasystem.kafka.impl;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.repository.KafkaTopicRepository;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaClusterProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaTopicProcessService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Transactional
public class KafkaTopicProcessServiceImpl implements KafkaTopicProcessService, DataSystemMetadataService<KafkaClusterDO> {

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private KafkaClusterProcessService kafkaClusterProcessService;

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    private KafkaTopicRepository kafkaTopicRepository;

    @Autowired
    private KafkaHelperService kafkaHelperService;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Page<KafkaTopicDTO> queryKafkaTopic(final KafkaTopicDTO kafkaTopic) {

        KafkaClusterDO kafkaCluster = kafkaClusterRepository.findById(kafkaTopic.getKafkaClusterId())
                .orElseThrow(() -> new NotFoundException(
                        ErrorMsg.E_109,
                        String.format("clusterId: %s", kafkaTopic.getKafkaClusterId())));
        KafkaTopicDO query = KafkaTopicDO.builder()
                .kafkaCluster(kafkaCluster)
                .name(kafkaTopic.getName())
                .build();

        Pageable pageable = PagedQuery.ofPage(kafkaTopic.getCurrent(), kafkaTopic.getPageSize());
        return kafkaTopicService.query(query, pageable).map(KafkaTopicDTO::new);
    }

    @Override
    public KafkaTopicDTO getKafkaTopic(final Long id) {
        return kafkaTopicRepository.findById(id)
                .map(KafkaTopicDTO::toKafkaTopicDTO)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public void refreshMetadata() {
        // 只同步clusterType为USER的集群
        KafkaClusterQuery query = KafkaClusterQuery.builder().clusterType(KafkaClusterType.USER).build();
        List<KafkaClusterDO> kafkaClusters = kafkaClusterRepository.queryAll(query);
        if (CollectionUtils.isEmpty(kafkaClusters)) {
            return;
        }
        this.refreshMetadata(kafkaClusters);
    }

    @Override
    public void refreshMetadata(final List<KafkaClusterDO> kafkaClusters) {
        if (CollectionUtils.isEmpty(kafkaClusters)) {
            return;
        }
        kafkaClusters.forEach(kafkaCluster -> {
            Map<String, Object> adminConfig = kafkaClusterProcessService.getAdminConfig(kafkaCluster.getId());
            Set<String> topics = kafkaHelperService.listTopics(adminConfig);
            if (!CollectionUtils.isEmpty(topics)) {
                diffKafkaTopic(topics, kafkaCluster);
            }
        });
    }

    protected void diffKafkaTopic(final Set<String> topics, final KafkaClusterDO kafkaCluster) {
        KafkaTopicDO query = KafkaTopicDO.builder().deleted(false).kafkaCluster(kafkaCluster).build();
        List<KafkaTopicDO> topicDOList = kafkaTopicService.queryAll(query);
        List<KafkaTopicDO> toDeleteTopics = Lists.newArrayList();
        List<KafkaTopicDO> toInsertTopics = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(topicDOList)) {
            // 过滤在数据库中有，从kafka没查询到的（需要从数据库删除）
            Map<String, KafkaTopicDO> topicMap = topicDOList.stream().collect(Collectors.toMap(KafkaTopicDO::getName, Function.identity()));
            toDeleteTopics = topicDOList.stream().filter(topic -> !topics.contains(topic.getName()))
                    .peek(topic -> topic.setDeleted(true)).collect(Collectors.toList());

            // 过滤在kafka中有，数据库中没有的（需要添加到数据库）
            toInsertTopics = topics.stream().filter(topic -> !topicMap.containsKey(topic))
                    .map(topic -> buildKafkaTopicDO(kafkaCluster, topic)).collect(Collectors.toList());
        } else {
            for (String topic : topics) {
                toInsertTopics.add(buildKafkaTopicDO(kafkaCluster, topic));
            }
        }

        // Merge
        List<KafkaTopicDO> toMergeTopics = Lists.newArrayListWithCapacity(toDeleteTopics.size() + toInsertTopics.size());
        toMergeTopics.addAll(toDeleteTopics);
        toMergeTopics.addAll(toInsertTopics);
        if (!CollectionUtils.isEmpty(toMergeTopics)) {
            kafkaTopicService.saveAll(toMergeTopics);
        }
    }

    @NotNull
    private KafkaTopicDO buildKafkaTopicDO(final KafkaClusterDO kafkaCluster, final String topic) {
        return KafkaTopicDO.builder().kafkaCluster(kafkaCluster).name(topic).build();
    }
}
