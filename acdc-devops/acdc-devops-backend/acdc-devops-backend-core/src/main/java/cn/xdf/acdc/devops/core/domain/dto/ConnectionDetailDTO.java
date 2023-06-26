package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectionDetailDTO {
    
    private Long id;
    
    private DataSystemType sourceDataSystemType;
    
    private Long sourceProjectId;
    
    private String sourceProjectName;
    
    private Long sourceDataSystemClusterId;
    
    private String sourceDataSystemClusterName;
    
    private Long sourceInstanceId;
    
    private String sourceInstanceHost;
    
    private String sourceInstanceVip;
    
    private String sourceInstancePort;
    
    private Long sourceDatabaseId;
    
    private String sourceDatabaseName;
    
    private Long sourceDataCollectionId;
    
    private String sourceDataCollectionName;
    
    private String sourceDataCollectionTopicName;
    
    private DataSystemType sinkDataSystemType;
    
    private Long sinkProjectId;
    
    private String sinkProjectName;
    
    private Long sinkDataSystemClusterId;
    
    private String sinkDataSystemClusterName;
    
    private Long sinkInstanceId;
    
    private String sinkInstanceHost;
    
    private String sinkInstanceVip;
    
    private String sinkInstancePort;
    
    private Long sinkDatabaseId;
    
    private String sinkDatabaseName;
    
    private Long sinkDataCollectionId;
    
    private String sinkDataCollectionName;
    
    private Long userId;
    
    private String userEmail;
    
    private String specificConfiguration;
    
    private Long sourceConnectorId;
    
    private String sourceConnectorName;
    
    private Long sinkConnectorId;
    
    private String sinkConnectorName;
    
    private Integer version = 1;
    
    private RequisitionState requisitionState = RequisitionState.APPROVING;
    
    private ConnectionState desiredState = ConnectionState.STOPPED;
    
    private ConnectionState actualState = ConnectionState.STOPPED;
    
    private Date updateTime;
    
    private Date creationTime;
    
    private Boolean deleted = Boolean.FALSE;
    
    private List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
    
    public ConnectionDetailDTO(final ConnectionDO connection) {
        this.id = connection.getId();
        this.sourceDataSystemType = connection.getSourceDataSystemType();
        this.sourceProjectId = connection.getSourceProject().getId();
        this.sourceProjectName = connection.getSourceProject().getName();
        
        // source database, cluster
        switch (sourceDataSystemType) {
            case MYSQL:
            case TIDB:
                if (Objects.nonNull(connection.getSourceDataCollection().getParentResource())) {
                    this.sourceDatabaseName = connection.getSourceDataCollection().getParentResource().getName();
                    this.sourceDataSystemClusterName = connection.getSourceDataCollection().getParentResource().getParentResource().getName();
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("unsupported source data system type: %s", sourceDataSystemType));
        }
        
        this.sourceDataCollectionId = connection.getSourceDataCollection().getId();
        this.sourceDataCollectionName = connection.getSourceDataCollection().getName();
        if (Objects.nonNull(connection.getSourceConnector())) {
            this.sourceConnectorId = connection.getSourceConnector().getId();
            this.sourceConnectorName = connection.getSourceConnector().getName();
            this.sourceDataCollectionTopicName = connection.getSourceDataCollection().getKafkaTopic().getName();
        }
        
        this.sinkDataSystemType = connection.getSinkDataSystemType();
        this.sinkProjectId = connection.getSinkProject().getId();
        this.sinkProjectName = connection.getSinkProject().getName();
        this.sinkDataCollectionId = connection.getSinkDataCollection().getId();
        this.sinkDataCollectionName = connection.getSinkDataCollection().getName();
        
        // sink cluster
        switch (sinkDataSystemType) {
            case MYSQL:
            case TIDB:
            case HIVE:
                if (Objects.nonNull(connection.getSinkDataCollection().getParentResource())) {
                    this.sinkDatabaseName = connection.getSinkDataCollection().getParentResource().getName();
                    this.sinkDataSystemClusterName = connection.getSinkDataCollection().getParentResource().getParentResource().getName();
                }
                break;
            case KAFKA:
            case ELASTIC_SEARCH:
                if (Objects.nonNull(connection.getSinkDataCollection().getParentResource())) {
                    this.sinkDataSystemClusterName = connection.getSinkDataCollection().getParentResource().getName();
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("unsupported sink data system type: %s", sourceDataSystemType));
        }
        
        this.specificConfiguration = connection.getSpecificConfiguration();
        if (Objects.nonNull(connection.getSinkConnector())) {
            this.sinkConnectorId = connection.getSinkConnector().getId();
            this.sinkConnectorName = connection.getSinkConnector().getName();
        }
        this.version = connection.getVersion();
        this.requisitionState = connection.getRequisitionState();
        this.desiredState = connection.getDesiredState();
        this.actualState = connection.getActualState();
        this.userId = connection.getUser().getId();
        this.userEmail = connection.getUser().getEmail();
        this.deleted = connection.getDeleted();
        this.creationTime = connection.getCreationTime();
        this.updateTime = connection.getUpdateTime();
        
        if (Objects.nonNull(connection.getSinkInstance())) {
            this.sinkInstanceId = connection.getSinkInstance().getId();
        }
        
        connection.getConnectionColumnConfigurations().forEach(each -> {
            connectionColumnConfigurations.add(new ConnectionColumnConfigurationDTO(each));
        });
    }
    
    /**
     * Convert to DO.
     *
     * @return ConnectionDO
     */
    public ConnectionDO toDO() {
        Set<ConnectionColumnConfigurationDO> connectionColumnConfigurationDOs = Sets.newHashSet();
        for (ConnectionColumnConfigurationDTO columnConfigurationDTO : this.connectionColumnConfigurations) {
            ConnectionColumnConfigurationDO columnConfigurationDO = columnConfigurationDTO.toDO();
            columnConfigurationDO.setConnectionVersion(this.version);
            connectionColumnConfigurationDOs.add(columnConfigurationDO);
        }
        
        ConnectionDO connectionDO = new ConnectionDO();
        connectionDO.setId(this.id);
        connectionDO.setSourceDataSystemType(this.getSourceDataSystemType());
        connectionDO.setSinkDataSystemType(this.getSinkDataSystemType());
        connectionDO.setSourceProject(new ProjectDO().setId(this.getSourceProjectId()));
        connectionDO.setSinkProject(new ProjectDO().setId(this.getSinkProjectId()));
        connectionDO.setSourceDataCollection(new DataSystemResourceDO(this.getSourceDataCollectionId()));
        connectionDO.setSinkDataCollection(new DataSystemResourceDO(this.getSinkDataCollectionId()));
        connectionDO.setSpecificConfiguration(this.getSpecificConfiguration());
        connectionDO.setUser(new UserDO().setId(this.getUserId()));
        connectionDO.setDeleted(this.getDeleted());
        connectionDO.setRequisitionState(this.getRequisitionState());
        connectionDO.setActualState(this.getActualState());
        connectionDO.setDesiredState(this.getDesiredState());
        connectionDO.setVersion(this.getVersion());
        connectionDO.setConnectionColumnConfigurations(connectionColumnConfigurationDOs);
        connectionDO.setCreationTime(this.creationTime);
        connectionDO.setUpdateTime(this.updateTime);
        
        if (Objects.nonNull(this.getSourceConnectorId())) {
            connectionDO.setSourceConnector(new ConnectorDO(this.getSourceConnectorId()));
        }
        
        if (Objects.nonNull(this.getSinkConnectorId())) {
            connectionDO.setSinkConnector(new ConnectorDO(this.getSinkConnectorId()));
        }
        
        if (Objects.nonNull(this.getSinkInstanceId())) {
            connectionDO.setSinkInstance(new DataSystemResourceDO(this.getSinkInstanceId()));
        }
        // cascading save
        connectionColumnConfigurationDOs.forEach(it -> it.setConnection(connectionDO));
        
        return connectionDO;
    }
}
