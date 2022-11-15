package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class KafkaTopicDTO extends PageDTO {

    private Long id;

    private Long kafkaClusterId;

    private String name;

    public KafkaTopicDTO(final KafkaTopicDO kafkaTopic) {
        this.id = kafkaTopic.getId();
        this.name = kafkaTopic.getName();
        this.kafkaClusterId = kafkaTopic.getKafkaCluster().getId();
    }

    /**
     * convert DO to DTO.
     *
     * @param kafkaTopicDO kafka topic DO
     * @return cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO
     * @date 2022/9/19 11:37 上午
     */
    public static KafkaTopicDTO toKafkaTopicDTO(final KafkaTopicDO kafkaTopicDO) {
        return KafkaTopicDTO.builder()
                .id(kafkaTopicDO.getId())
                .kafkaClusterId(kafkaTopicDO.getKafkaCluster().getId())
                .name(kafkaTopicDO.getName())
                .build();
    }
}
