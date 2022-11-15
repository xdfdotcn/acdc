package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class KafkaClusterQuery extends PagedQuery {

    private String bootstrapServers;

    private KafkaClusterType clusterType;

    private List<Long> kafkaClusterIds;

    private String name;

    private Long projectId;
}
