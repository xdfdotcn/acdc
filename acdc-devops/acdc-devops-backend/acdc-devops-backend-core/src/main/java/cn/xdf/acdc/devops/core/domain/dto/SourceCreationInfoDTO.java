package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor

public class SourceCreationInfoDTO {

    // 集群ID
    private Long clusterId;

    // 实例ID
    private Long instanceId;

    // 数据库ID
    private Long databaseId;

    // 表ID
    private Long dataSetId;

}
