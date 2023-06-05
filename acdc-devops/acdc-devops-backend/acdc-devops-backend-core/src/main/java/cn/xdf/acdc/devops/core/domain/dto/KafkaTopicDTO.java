package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
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
        return new KafkaTopicDO()
                .setId(this.id)
                .setName(this.name);
    }
}
