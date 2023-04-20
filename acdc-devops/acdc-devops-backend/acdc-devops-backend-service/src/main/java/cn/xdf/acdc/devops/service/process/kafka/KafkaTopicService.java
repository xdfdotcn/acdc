package cn.xdf.acdc.devops.service.process.kafka;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDetailDTO;
import cn.xdf.acdc.devops.core.domain.query.KafkaTopicQuery;
import org.springframework.data.domain.Page;

import java.util.List;

public interface KafkaTopicService {

    /**
     * Create kafka topic.
     *
     * @param kafkaTopic KafkaTopic
     * @return Kafka topic
     */
    KafkaTopicDetailDTO create(KafkaTopicDetailDTO kafkaTopic);

    /**
     * Create kafka topic.
     *
     * @param kafkaTopics kafkaTopics
     * @return created kafka topic list
     */
    List<KafkaTopicDetailDTO> batchCreate(List<KafkaTopicDetailDTO> kafkaTopics);

    /**
     * Paged query kafka topic list.
     *
     * @param query query
     * @return kafka topic page list
     */
    Page<KafkaTopicDTO> pagedQuery(KafkaTopicQuery query);

    /**
     * Query kafka topic list.
     *
     * @param query query
     * @return kafka topic list
     */
    List<KafkaTopicDTO> query(KafkaTopicQuery query);

    /**
     * Get kafka topic.
     *
     * @param id id
     * @return Kafka topic
     */
    KafkaTopicDTO getById(Long id);


    /**
     * Create kafka topic in ACDC and kafka cluster.
     *
     * @param dataCollectionResourceId data collection resource id
     * @param kafkaClusterId             kafka cluster id
     * @param topicName                  topic name
     * @return created kafka topic detail DTO
     */
    KafkaTopicDetailDTO createDataCollectionTopicIfAbsent(Long dataCollectionResourceId, Long kafkaClusterId, String topicName);

    /**
     * Create TICDC kafka topic.
     *
     * @param ticdcTopicName     ticdc topic name
     * @param kafkaClusterId     kafka cluster id
     * @param databaseResourceId database resource id
     * @return created kafka topic detail DTO
     */
    KafkaTopicDetailDTO createTICDCTopicIfAbsent(String ticdcTopicName, Long kafkaClusterId, Long databaseResourceId);
}
