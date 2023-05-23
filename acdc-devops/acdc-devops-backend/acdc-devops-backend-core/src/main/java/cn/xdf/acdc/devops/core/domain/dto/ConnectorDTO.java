package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectorDTO {
    
    private Long id;
    
    private String name;
    
    private Date creationTime;
    
    private Date updateTime;
    
    private ConnectorState desiredState;
    
    private ConnectorState actualState;
    
    private ConnectorType connectorType;
    
    private DataSystemType dataSystemType;
    
    public ConnectorDTO(final ConnectorDO connector) {
        this.id = connector.getId();
        this.name = connector.getName();
        this.actualState = connector.getActualState();
        
        this.desiredState = connector.getDesiredState();
        
        this.creationTime = connector.getCreationTime();
        this.updateTime = connector.getUpdateTime();
        
        if (Objects.nonNull(connector.getConnectorClass())) {
            this.connectorType = connector.getConnectorClass().getConnectorType();
            this.dataSystemType = connector.getConnectorClass().getDataSystemType();
        }
    }
}
