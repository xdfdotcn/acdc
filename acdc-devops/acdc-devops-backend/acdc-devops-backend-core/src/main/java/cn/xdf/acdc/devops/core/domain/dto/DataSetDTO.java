package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataSetDTO {

    private DataSystemType dataSystemType;

    private Long projectId;

    private Long clusterId;

    private String clusterName;

    private Long instanceId;

    private String instanceName;

    private Long databaseId;

    private String databaseName;

    private Long dataSetId;

    private String specificConfiguration;

    private String datasetName;
}
