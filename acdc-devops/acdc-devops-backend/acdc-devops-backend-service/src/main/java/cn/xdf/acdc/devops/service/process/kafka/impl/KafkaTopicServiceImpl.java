package cn.xdf.acdc.devops.service.process.kafka.impl;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.query.KafkaTopicQuery;
import cn.xdf.acdc.devops.repository.KafkaTopicRepository;
import cn.xdf.acdc.devops.service.config.TopicProperties;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Kafka;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.collect.Sets;
import joptsimple.internal.Strings;
import org.apache.kafka.common.acl.AclOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class KafkaTopicServiceImpl implements KafkaTopicService {

    @Autowired
    private I18nService i18n;

    @Autowired
    private KafkaTopicRepository kafkaTopicRepository;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaHelperService kafkaHelperService;

    @Autowired
    private TopicProperties topicProperties;

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    @Transactional
    public KafkaTopicDetailDTO create(final KafkaTopicDetailDTO kafkaTopic) {
        checkKafkaTopicDetail(kafkaTopic);

        KafkaTopicDO kafkaTopicDO = kafkaTopic.toDO();
        return new KafkaTopicDetailDTO(kafkaTopicRepository.save(kafkaTopicDO));
    }

    @Override
    @Transactional
    public List<KafkaTopicDetailDTO> batchCreate(final List<KafkaTopicDetailDTO> kafkaTopics) {
        Set<KafkaTopicDO> toSaveKafkaTopicDOs = Sets.newHashSet();
        for (KafkaTopicDetailDTO detail : kafkaTopics) {
            checkKafkaTopicDetail(detail);
            KafkaTopicDO kafkaTopicDO = detail.toDO();
            toSaveKafkaTopicDOs.add(kafkaTopicDO);
        }

        return kafkaTopicRepository.saveAll(toSaveKafkaTopicDOs).stream().map(KafkaTopicDetailDTO::new).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public Page<KafkaTopicDTO> pagedQuery(final KafkaTopicQuery query) {
        return kafkaTopicRepository.pagedQuery(query).map(KafkaTopicDTO::new);
    }

    @Override
    @Transactional
    public List<KafkaTopicDTO> query(final KafkaTopicQuery query) {
        return kafkaTopicRepository.pagedQuery(query).stream().map(KafkaTopicDTO::new).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public KafkaTopicDTO getById(final Long id) {
        return kafkaTopicRepository.findByDeletedFalseAndId(id).map(KafkaTopicDTO::new).orElseThrow(() -> new EntityNotFoundException(i18n.msg(Kafka.TOPIC_NOT_FOUND, id)));
    }

    @Override
    @Transactional
    public KafkaTopicDetailDTO createDataCollectionTopicIfAbsent(final Long dataCollectionResourceId, final Long kafkaClusterId, final String topicName) {
        Optional<KafkaTopicDO> kafkaTopicOpt = kafkaTopicRepository.findByDataSystemResourceId(dataCollectionResourceId);

        if (kafkaTopicOpt.isPresent()) {
            return new KafkaTopicDetailDTO(kafkaTopicOpt.get());
        }

        KafkaTopicDetailDTO kafkaTopicDetailDTO = new KafkaTopicDetailDTO()
                .setName(topicName)
                .setKafkaClusterId(kafkaClusterId)
                .setDataSystemResourceId(dataCollectionResourceId);

        KafkaTopicDetailDTO createdKafkaTopicDTO = create(kafkaTopicDetailDTO);

        createDataCollectionTopicInKafkaClusterIfAbsent(
                topicName,
                topicProperties.getDataCollection().getPartitions(),
                topicProperties.getDataCollection().getReplicationFactor(),
                topicProperties.getDataCollection().getConfigs(), kafkaClusterId);

        // TODO 后续优雅实现
        entityManager.flush();
        entityManager.clear();

        return createdKafkaTopicDTO;
    }

    private void createDataCollectionTopicInKafkaClusterIfAbsent(
            final String topic,
            final int partitions,
            final short replicationFactor,
            final Map<String, String> topicConfig,
            final Long kafkaClusterId
    ) {
        Map<String, Object> adminConfig = kafkaClusterService.getDecryptedAdminConfig(kafkaClusterId);
        kafkaHelperService.createTopic(topic, partitions, replicationFactor, topicConfig, adminConfig);
    }

    @Override
    @Transactional
    public KafkaTopicDetailDTO createTICDCTopicIfAbsent(final String ticdcTopicName, final Long kafkaClusterId, final Long databaseResourceId) {
        Optional<KafkaTopicDO> kafkaTopicOpt = kafkaTopicRepository.findByDataSystemResourceId(databaseResourceId);

        if (kafkaTopicOpt.isPresent()) {
            return new KafkaTopicDetailDTO(kafkaTopicOpt.get());
        }

        KafkaTopicDetailDTO kafkaTopicDetailDTO = new KafkaTopicDetailDTO()
                .setName(ticdcTopicName)
                .setKafkaClusterId(kafkaClusterId)
                .setDataSystemResourceId(databaseResourceId);

        KafkaTopicDetailDTO createdKafkaTopicDTO = create(kafkaTopicDetailDTO);

        createAuthorizedTICDCTopicInKafkaClusterIfAbsent(ticdcTopicName, kafkaClusterId);

        // TODO 后续优雅实现
        entityManager.flush();
        entityManager.clear();

        return createdKafkaTopicDTO;
    }

    private void createAuthorizedTICDCTopicInKafkaClusterIfAbsent(final String topic, final Long kafkaClusterId) {
        Map<String, Object> adminConfig = kafkaClusterService.getDecryptedAdminConfig(kafkaClusterId);

        kafkaHelperService.createTopic(topic, topicProperties.getTicdc().getPartitions(), topicProperties.getTicdc().getReplicationFactor(), topicProperties.getTicdc().getConfigs(), adminConfig);

        // add ACL
        String username = topicProperties.getTicdc().getAcl().getUsername();
        String[] operations = topicProperties.getTicdc().getAcl().getOperations();
        for (String operation : operations) {
            AclOperation aclOperation = AclOperation.valueOf(operation);
            kafkaHelperService.addAcl(topic, username, aclOperation, adminConfig);
        }
    }

    private void checkKafkaTopicDetail(final KafkaTopicDetailDTO kafkaTopic) {
        if (Objects.isNull(kafkaTopic) || Strings.isNullOrEmpty(kafkaTopic.getName()) || Objects.isNull(kafkaTopic.getKafkaClusterId()) || Objects.isNull(kafkaTopic.getDataSystemResourceId())) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
        }
    }
}
