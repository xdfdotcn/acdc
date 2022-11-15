package cn.xdf.acdc.devops.core.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// TODO：重命名为 DTO
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SinkConnectorInfoDO {

    private Long connectorId;

    private String name;

    private String kafkaTopic;

    private String sinkCluster;

    private String sinkDatabase;

    private String sinkDataSet;

    private String sinkDataSystemType;

    private String sinkClusterType;

    private String srcCluster;

    private String srcDatabase;

    private Long srcDataSetId;

    private String srcDataSet;

    private String srcDataSystemType;
}
