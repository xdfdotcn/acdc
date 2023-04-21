package cn.xdf.acdc.devops.dto;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Connector 信息, 配置相关.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Connector {

    private Long id;

    private String connectClusterUrl;

    private String name;

    private Map<String, String> connectorConfig;

    private ConnectorState actualState;

    private ConnectorState desiredState;

    private String remark = "";

    public Connector(
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

    public Connector(final ConnectorDO connector, final Map<String, String> configMap) {
        this.id = connector.getId();
        this.name = connector.getName();
        this.actualState = connector.getActualState();
        this.desiredState = connector.getDesiredState();
        this.connectClusterUrl = connector.getConnectCluster().getConnectRestApiUrl();
        this.connectorConfig = configMap;
    }

    /**
     * Generate a ConnectorInfoDTO from ConnectorDetailDTO.
     *
     * @param connectorDetail connectorDetail
     * @return ConnectorInfoDTO
     */
    public static Connector fromConnectorDetail(final ConnectorDetailDTO connectorDetail) {
        Map<String, String> connectorConfig = connectorDetail.getConnectorConfigurations().stream()
                .collect(Collectors.toMap(ConnectorConfigurationDTO::getName, ConnectorConfigurationDTO::getValue));
        Connector result = new Connector();
        result.setId(connectorDetail.getId());
        result.setConnectClusterUrl(connectorDetail.getConnectClusterRestApiUrl());
        result.setName(connectorDetail.getName());
        result.setConnectorConfig(connectorConfig);
        result.setDesiredState(connectorDetail.getDesiredState());
        result.setActualState(connectorDetail.getActualState());
        return result;
    }

    /**
     * For unit test.
     *
     * @return ConnectorDetailDTO
     */
    public ConnectorDetailDTO toDetailDTO() {
        List<ConnectorConfigurationDTO> connectorConfigurations = new ArrayList<>();
        if (Objects.nonNull(connectorConfig)) {
            connectorConfigurations = connectorConfig.entrySet().stream()
                    .map(entry -> ConnectorConfigurationDTO.builder()
                            .name(entry.getKey())
                            .value(entry.getValue())
                            .build())
                    .collect(Collectors.toList());
        }
        return ConnectorDetailDTO.builder()
                .id(this.id)
                .name(this.name)
                .connectClusterRestApiUrl(this.connectClusterUrl)
                .connectorConfigurations(connectorConfigurations)
                .actualState(this.actualState)
                .desiredState(this.desiredState)
                .build();

    }
}
