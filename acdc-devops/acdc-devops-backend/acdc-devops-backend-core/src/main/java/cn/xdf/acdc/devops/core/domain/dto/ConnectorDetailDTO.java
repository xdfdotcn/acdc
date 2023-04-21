package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectorDetailDTO {

    private Long id;

    private String name;

    private Date creationTime;

    private Date updateTime;

    private ConnectorState desiredState;

    private ConnectorState actualState;

    private Long kafkaClusterId;

    private Long connectorClassId;

    private Long dataSystemResourceId;

    private String dataSystemResourceName;

    private String dataSystemClusterName;

    private ConnectorType connectorType;

    private DataSystemType dataSystemType;

    private Long connectClusterId;

    private String connectClusterRestApiUrl;

    private List<ConnectorConfigurationDTO> connectorConfigurations = new ArrayList<>();

    private Long connectionIdWithThisAsSinkConnector;

    public ConnectorDetailDTO(final ConnectorDO connector) {
        this.id = connector.getId();
        this.name = connector.getName();
        this.actualState = connector.getActualState();
        this.desiredState = connector.getDesiredState();

        this.creationTime = connector.getCreationTime();
        this.updateTime = connector.getUpdateTime();

        this.kafkaClusterId = connector.getKafkaCluster().getId();
        this.connectorClassId = connector.getConnectorClass().getId();
        this.dataSystemResourceId = connector.getDataSystemResource().getId();
        this.dataSystemResourceName = connector.getDataSystemResource().getName();

        DataSystemResourceDO current = connector.getDataSystemResource();
        while (current.getParentResource() != null) {
            current = current.getParentResource();
        }
        dataSystemClusterName = current.getName();

        this.connectorType = connector.getConnectorClass().getConnectorType();
        this.dataSystemType = connector.getConnectorClass().getDataSystemType();

        this.connectClusterId = connector.getConnectCluster().getId();
        this.connectClusterRestApiUrl = connector.getConnectCluster().getConnectRestApiUrl();
        if (Objects.nonNull(connector.getConnectionWithThisAsSinkConnector())) {
            this.connectionIdWithThisAsSinkConnector = connector.getConnectionWithThisAsSinkConnector().getId();
        }

        connector.getConnectorConfigurations().forEach(each -> connectorConfigurations.add(new ConnectorConfigurationDTO(each)));
    }

    /**
     * Convert to DO.
     *
     * @return ConnectorDO
     */
    public ConnectorDO toDO() {
        Set<ConnectorConfigurationDO> connectorConfigurations = new HashSet<>();
        this.connectorConfigurations.forEach(each -> connectorConfigurations.add(each.toDO()));

        ConnectorDO connectorDO = ConnectorDO.builder()
                .id(id)
                .name(name)
                .actualState(actualState)
                .desiredState(desiredState)
                .connectCluster(new ConnectClusterDO(connectClusterId))
                .kafkaCluster(new KafkaClusterDO(kafkaClusterId))
                .connectorClass(new ConnectorClassDO(connectorClassId))
                .dataSystemResource(new DataSystemResourceDO(dataSystemResourceId))
                .connectorConfigurations(connectorConfigurations)
                .build();

        // for cascade
        connectorConfigurations.forEach(each -> each.setConnector(connectorDO));

        return connectorDO;
    }
}
