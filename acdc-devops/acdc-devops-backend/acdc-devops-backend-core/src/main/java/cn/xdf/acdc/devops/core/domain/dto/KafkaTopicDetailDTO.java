package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class KafkaTopicDetailDTO {

    private Long id;

    private String name;

    private Long kafkaClusterId;

    private Long dataSystemResourceId;

    public KafkaTopicDetailDTO(final KafkaTopicDO kafkaTopicDO) {
        this.id = kafkaTopicDO.getId();
        this.name = kafkaTopicDO.getName();
        this.kafkaClusterId = kafkaTopicDO.getKafkaCluster().getId();
        this.dataSystemResourceId = kafkaTopicDO.getDataSystemResource().getId();
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
                .kafkaCluster(new KafkaClusterDO(kafkaClusterId))
                .dataSystemResource(new DataSystemResourceDO(dataSystemResourceId))
                .build();
    }
}
