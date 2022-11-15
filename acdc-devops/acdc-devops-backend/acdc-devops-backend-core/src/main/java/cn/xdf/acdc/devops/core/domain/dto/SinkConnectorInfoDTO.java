package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sink connector.
 */
// CHECKSTYLE:OFF
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SinkConnectorInfoDTO extends PageDTO {

    public static final String SORT_FIELD = "sink.id";

    private Long connectorId;

    private Long sourceConnectorId;

    private String sinkDataSystemType;

    private String name;

    private String kafkaTopic;

    private String sinkCluster;

    private String sinkClusterType;

    private String sinkDatabase;

    private String sinkDataSet;

    private String srcCluster;

    private String srcDataSystemType;

    private String srcDatabase;

    private String srcDataSet;

    private Long srcDataSetId;

    public SinkConnectorInfoDTO(final SinkConnectorInfoDO sinkConnectorInfo) {
        this.connectorId = sinkConnectorInfo.getConnectorId();
        this.name = sinkConnectorInfo.getName();
        this.kafkaTopic = sinkConnectorInfo.getKafkaTopic();

        this.sinkDataSet = sinkConnectorInfo.getSinkDataSet();
        this.sinkDatabase = sinkConnectorInfo.getSinkDatabase();
        this.sinkCluster = sinkConnectorInfo.getSinkCluster();
        this.sinkClusterType = sinkConnectorInfo.getSinkClusterType();
        this.sinkDataSystemType = sinkConnectorInfo.getSinkDataSystemType();

        this.srcCluster = sinkConnectorInfo.getSrcCluster();
        this.srcDatabase = sinkConnectorInfo.getSrcDatabase();
        this.srcDataSet = sinkConnectorInfo.getSrcDataSet();
        this.srcDataSetId = sinkConnectorInfo.getSrcDataSetId();
        this.srcDataSystemType = sinkConnectorInfo.getSrcDataSystemType();
    }


    public SinkConnectorInfoDTO(
            final Long connectorId,
            final String name,
            final String kafkaTopic,
            final String sinkCluster,
            final String sinkDatabase,
            final String sinkDataSet) {

        this.connectorId = connectorId;
        this.name = name;
        this.kafkaTopic = kafkaTopic;
        this.sinkCluster = sinkCluster;
        this.sinkDatabase = sinkDatabase;
        this.sinkDataSet = sinkDataSet;
    }
}
