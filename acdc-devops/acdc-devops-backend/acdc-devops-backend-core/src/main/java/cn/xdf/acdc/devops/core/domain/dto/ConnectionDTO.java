package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectionDTO {

    private Long id;

    private DataSystemType sourceDataSystemType;

    private Long sourceProjectId;

    private Long sourceDataSetId;

    private Long sourceConnectorId;

    private DataSystemType sinkDataSystemType;

    private Long sinkProjectId;

    private Long sinkInstanceId;

    private Long sinkDataSetId;

    private Long sinkConnectorId;

    private Integer version;

    public ConnectionDTO(final ConnectionDO connectionDO) {
        this.id = connectionDO.getId();
        this.sourceDataSystemType = connectionDO.getSourceDataSystemType();
        this.sourceProjectId = connectionDO.getSourceProject().getId();
        this.sourceDataSetId = connectionDO.getSourceDataSetId();
        if (Objects.nonNull(connectionDO.getSourceConnector())) {
            this.sourceConnectorId = connectionDO.getSourceConnector().getId();
        }
        this.sinkDataSystemType = connectionDO.getSinkDataSystemType();
        this.sinkProjectId = connectionDO.getSinkProject().getId();
        this.sinkInstanceId = connectionDO.getSinkInstanceId();
        this.sinkDataSetId = connectionDO.getSinkDataSetId();
        if (Objects.nonNull(connectionDO.getSinkConnector())) {
            this.sinkConnectorId = connectionDO.getSinkConnector().getId();
        }
        this.version = connectionDO.getVersion();
    }
}
