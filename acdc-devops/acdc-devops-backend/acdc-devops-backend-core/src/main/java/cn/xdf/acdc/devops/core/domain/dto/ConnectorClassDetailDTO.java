package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectorClassDetailDTO {
    
    private Long id;
    
    private String name;
    
    private String simpleName;
    
    private String description;
    
    private Set<DefaultConnectorConfigurationDTO> defaultConnectorConfigurations = new HashSet<>();
    
    private ConnectorType connectorType;
    
    private DataSystemType dataSystemType;
    
    private List<ConnectClusterDTO> connectClusters = Lists.newArrayList();
    
    public ConnectorClassDetailDTO(final ConnectorClassDO connectorClass) {
        this.id = connectorClass.getId();
        this.name = connectorClass.getName();
        this.simpleName = connectorClass.getSimpleName();
        this.description = connectorClass.getDescription();
        this.connectorType = connectorClass.getConnectorType();
        this.dataSystemType = connectorClass.getDataSystemType();
        
        connectorClass.getDefaultConnectorConfigurations().forEach(each -> defaultConnectorConfigurations.add(new DefaultConnectorConfigurationDTO(each)));
        connectorClass.getConnectClusters().forEach(it -> connectClusters.add(new ConnectClusterDTO(it)));
    }
}
