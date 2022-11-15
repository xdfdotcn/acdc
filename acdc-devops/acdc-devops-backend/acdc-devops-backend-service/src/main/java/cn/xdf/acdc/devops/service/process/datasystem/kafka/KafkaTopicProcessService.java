package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import org.springframework.data.domain.Page;

public interface KafkaTopicProcessService {

    /**
     * 查询 kafka topic 分页.
     *
     * @param kafkaTopic kafkaTopic
     * @return Page
     */
    Page<KafkaTopicDTO> queryKafkaTopic(KafkaTopicDTO kafkaTopic);

    /**
     * Get kafka topic.
     *
     * @param id id
     * @return KafkaTopicDTO
     */
    KafkaTopicDTO getKafkaTopic(Long id);
}
