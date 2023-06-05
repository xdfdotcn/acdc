package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.LinkedList;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectionDTO {
    
    private Long id;
    
    private DataSystemType sourceDataSystemType;
    
    private Long sourceProjectId;
    
    private String sourceProjectName;
    
    private Long sourceDataCollectionId;
    
    private String sourceDataCollectionName;
    
    private String sourceDataCollectionTopicName;
    
    private Long sourceConnectorId;
    
    private DataSystemType sinkDataSystemType;
    
    private Long sinkProjectId;
    
    private String sinkProjectName;
    
    private Long sinkInstanceId;
    
    private Long sinkDataCollectionId;
    
    // 用于页面显示 sink data collection 的全路径
    private LinkedList<DataSystemResourceDTO> sinkDataCollectionResourcePath;
    
    private LinkedList<DataSystemResourceDTO> sourceDataCollectionResourcePath;
    
    private String sinkDataCollectionName;
    
    private String specificConfiguration;
    
    private Long sinkConnectorId;
    
    private String sinkConnectorName;
    
    private Integer version;
    
    private RequisitionState requisitionState;
    
    private ConnectionState actualState;
    
    private ConnectionState desiredState;
    
    private Long userId;
    
    private String userEmail;
    
    private boolean deleted;
    
    private Date creationTime;
    
    private Date updateTime;
    
    public ConnectionDTO(final ConnectionDO connection) {
        this.id = connection.getId();
        this.sourceDataSystemType = connection.getSourceDataSystemType();
        this.sourceProjectId = connection.getSourceProject().getId();
        this.sourceProjectName = connection.getSourceProject().getName();
        this.sourceDataCollectionId = connection.getSourceDataCollection().getId();
        this.sourceDataCollectionName = connection.getSourceDataCollection().getName();
        if (Objects.nonNull(connection.getSourceConnector())) {
            this.sourceConnectorId = connection.getSourceConnector().getId();
            this.sourceDataCollectionTopicName = connection.getSourceDataCollection().getKafkaTopic().getName();
        }
        this.sinkDataSystemType = connection.getSinkDataSystemType();
        this.sinkProjectId = connection.getSinkProject().getId();
        this.sinkProjectName = connection.getSinkProject().getName();
        this.sinkDataCollectionId = connection.getSinkDataCollection().getId();
        
        // path of  data collection
        sinkDataCollectionResourcePath = generateResourcePathFromDataCollection(connection.getSinkDataCollection());
        sourceDataCollectionResourcePath = generateResourcePathFromDataCollection(connection.getSourceDataCollection());
        
        this.sinkDataCollectionName = connection.getSinkDataCollection().getName();
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
    }
    
    private LinkedList<DataSystemResourceDTO> generateResourcePathFromDataCollection(final DataSystemResourceDO dataCollectionDO) {
        LinkedList<DataSystemResourceDTO> dataCollectionResourcePath = new LinkedList<>();
        
        for (DataSystemResourceDO current = dataCollectionDO; null != current; current = current.getParentResource()) {
            dataCollectionResourcePath.push(new DataSystemResourceDTO(current));
        }
        
        return dataCollectionResourcePath;
    }
    
    /**
     * Convert to DO.
     *
     * @return ConnectionDO
     */
    public ConnectionDO toDO() {
        ConnectionDO connectionDO = new ConnectionDO();
        
        if (Objects.nonNull(sinkInstanceId)) {
            connectionDO.setSinkInstance(new DataSystemResourceDO(this.sinkInstanceId));
        }
        if (Objects.nonNull(sourceConnectorId)) {
            connectionDO.setSourceConnector(new ConnectorDO(this.sourceConnectorId));
        }
        if (Objects.nonNull(sinkConnectorId)) {
            connectionDO.setSinkConnector(new ConnectorDO(this.sinkConnectorId));
        }
        
        connectionDO.setCreationTime(this.creationTime);
        connectionDO.setUpdateTime(this.updateTime);
        connectionDO.setDeleted(this.deleted);
        connectionDO
                .setId(this.id)
                .setSourceDataSystemType(this.sourceDataSystemType)
                .setSourceProject(new ProjectDO(this.sourceProjectId))
                .setSourceDataCollection(new DataSystemResourceDO(this.sourceDataCollectionId))
                .setSinkDataSystemType(this.sinkDataSystemType)
                .setSinkProject(new ProjectDO(this.sinkProjectId))
                .setSinkDataCollection(new DataSystemResourceDO(this.sinkDataCollectionId))
                .setSpecificConfiguration(this.specificConfiguration)
                .setVersion(this.version)
                .setRequisitionState(this.requisitionState)
                .setDesiredState(this.desiredState)
                .setActualState(this.actualState)
                .setUser(new UserDO(this.userId));
        
        return connectionDO;
    }
}
