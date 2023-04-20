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

    private String name;

    public KafkaTopicDTO(final KafkaTopicDO kafkaTopic) {
        this.id = kafkaTopic.getId();
        this.name = kafkaTopic.getName();
    }

    /**
     * Convert to DO.
     *
     * @return KafkaTopicDO
     */
    public KafkaTopicDO toDO() {
        return KafkaTopicDO.builder()
                .id(this.id)
                .name(this.name)
                .build();
    }
}
