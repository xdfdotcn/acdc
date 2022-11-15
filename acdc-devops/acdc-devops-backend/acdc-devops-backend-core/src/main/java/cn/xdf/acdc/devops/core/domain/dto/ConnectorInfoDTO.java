package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import java.util.Map;

import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Connector 信息, 配置相关.
 */
@Getter
@Setter
@ToString
public class ConnectorInfoDTO {

    private Long id;

    private String connectClusterUrl;

    private String name;

    private Map<String, String> connectorConfig;

    private ConnectorState actualState;

    private ConnectorState desiredState;

    private String remark = "";

    public ConnectorInfoDTO() {
    }

    public ConnectorInfoDTO(
        final Long id,
        final String connectClusterUrl,
        final String name,
        final Map<String, String> connectorConfig,
        final ConnectorState actualState,
        final ConnectorState desiredState) {
        this.id = id;
        this.connectClusterUrl = connectClusterUrl;
        this.name = name;
        this.connectorConfig = connectorConfig;
        this.actualState = actualState;
        this.desiredState = desiredState;
    }

    public ConnectorInfoDTO(final ConnectorDO connector, final Map<String, String> configMap) {
        this.id = connector.getId();
        this.name = connector.getName();
        this.actualState = connector.getActualState();
        this.desiredState = connector.getDesiredState();
        this.connectClusterUrl = connector.getConnectCluster().getConnectRestApiUrl();
        this.connectorConfig = configMap;
    }
}
