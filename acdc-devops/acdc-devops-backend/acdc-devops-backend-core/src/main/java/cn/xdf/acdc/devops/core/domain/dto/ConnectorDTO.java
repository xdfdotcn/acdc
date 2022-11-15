package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectorDTO {

    private Long id;

    private String name;

    private String creationTimeFormat;

    private Instant creationTime;

    private String updateTimeFormat;

    private Instant updateTime;

    private String desiredStateName;

    private ConnectorState desiredState;

    private String actualStateName;

    private ConnectorState actualState;

    private String connectorTypeName;

    private ConnectorType connectorType;

    private String dataSystemTypeName;

    private DataSystemType dataSystemType;

    public ConnectorDTO(final ConnectorDO connector) {
        this.id = connector.getId();
        this.name = connector.getName();
        this.actualStateName = connector.getActualState().name();
        this.actualState = connector.getActualState();

        this.desiredStateName = connector.getDesiredState().name();
        this.desiredState = connector.getDesiredState();

        this.creationTimeFormat = DateUtil.formatToString(connector.getCreationTime());
        this.creationTime = connector.getCreationTime();

        this.updateTimeFormat = DateUtil.formatToString(connector.getUpdateTime());
        this.updateTime = connector.getUpdateTime();

        this.connectorTypeName = connector.getConnectorClass().getConnectorType().name();
        this.connectorType = connector.getConnectorClass().getConnectorType();
        this.dataSystemTypeName = connector.getConnectorClass().getDataSystemType().getName();
        this.dataSystemType = connector.getConnectorClass().getDataSystemType();
    }
}
