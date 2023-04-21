package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectorClassDTO {

    private Long id;

    private String name;

    private String simpleName;

    private String description;

    private ConnectorType connectorType;

    private DataSystemType dataSystemType;

    public ConnectorClassDTO(final ConnectorClassDO connectorClass) {
        this.id = connectorClass.getId();
        this.name = connectorClass.getName();
        this.simpleName = connectorClass.getSimpleName();
        this.description = connectorClass.getDescription();
        this.connectorType = connectorClass.getConnectorType();
        this.dataSystemType = connectorClass.getDataSystemType();
    }
}
