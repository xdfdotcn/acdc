package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.KafkaSpecificConfDTO;

public interface SpecificConfigurationProcessService {

    /**
     * Kafka specific config deserialize.
     * @param json  json string
     * @return KafkaSpecificConfDTO
     */
    KafkaSpecificConfDTO kafkaSpecificConfDeserialize(String json);
}
