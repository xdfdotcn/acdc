package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SourceConnectorInfoDTO {

    private Long connectorId;

    private String name;

    private String kafkaTopic;

    private String srcCluster;

    private String srcDataSystemType;

    private String srcDatabase;

    private Long srcDatabaseId;

    private String srcDataSet;
}
