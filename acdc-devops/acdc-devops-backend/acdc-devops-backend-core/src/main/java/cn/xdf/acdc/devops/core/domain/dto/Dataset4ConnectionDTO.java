package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Dataset4ConnectionDTO {

    private DataSystemType dataSystemType;

    private Long projectId;

    private String projectName;

    private Long clusterId;

    private String clusterName;

    private Long instanceId;

    private String instanceHost;

    private String instanceVIp;

    private String instancePort;

    private Long databaseId;

    private String databaseName;

    private Long dataSetId;

    private String datasetName;

    private String specificConfiguration;

}
