package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class DefaultConnectorConfigurationDTO {
    
    private Long id;
    
    private String name;
    
    private String value;
    
    private Long connectorClassId;
    
    public DefaultConnectorConfigurationDTO(final DefaultConnectorConfigurationDO defaultConnectorConfiguration) {
        this.id = defaultConnectorConfiguration.getId();
        this.name = defaultConnectorConfiguration.getName();
        this.value = defaultConnectorConfiguration.getValue();
        this.connectorClassId = defaultConnectorConfiguration.getConnectorClass().getId();
    }
}
