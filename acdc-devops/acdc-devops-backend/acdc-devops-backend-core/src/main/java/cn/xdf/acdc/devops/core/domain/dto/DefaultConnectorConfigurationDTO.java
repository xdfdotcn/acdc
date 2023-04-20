package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
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
