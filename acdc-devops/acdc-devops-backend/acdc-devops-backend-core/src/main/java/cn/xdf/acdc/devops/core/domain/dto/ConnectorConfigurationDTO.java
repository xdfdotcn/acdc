package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectorConfigurationDTO {
    
    private Long id;
    
    private String name;
    
    private String value;
    
    private Long connectorId;
    
    public ConnectorConfigurationDTO(final ConnectorConfigurationDO connectorConfigurationDO) {
        this.id = connectorConfigurationDO.getId();
        this.name = connectorConfigurationDO.getName();
        this.value = connectorConfigurationDO.getValue();
        this.connectorId = connectorConfigurationDO.getConnector().getId();
    }
    
    public ConnectorConfigurationDTO(final String name, final String value) {
        this.name = name;
        this.value = value;
    }
    
    /**
     * Convert to DO.
     *
     * @return ConnectorConfigurationDO
     */
    public ConnectorConfigurationDO toDO() {
        return new ConnectorConfigurationDO()
                .setId(id)
                .setName(name)
                .setValue(value)
                .setConnector(new ConnectorDO(connectorId));
    }
}
