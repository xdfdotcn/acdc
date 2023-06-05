package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectClusterDTO {
    
    private Long connectorClassId;
    
    private Long id;
    
    private String connectRestApiUrl;
    
    private String description;
    
    private String version;
    
    public ConnectClusterDTO(final ConnectClusterDO connectClusterDO) {
        this.id = connectClusterDO.getId();
        this.connectRestApiUrl = connectClusterDO.getConnectRestApiUrl();
        this.description = connectClusterDO.getDescription();
        this.version = connectClusterDO.getVersion();
        this.connectorClassId = connectClusterDO.getConnectorClass().getId();
    }
    
    /**
     * Convert to DO.
     *
     * @return ConnectClusterDO
     */
    public ConnectClusterDO toDO() {
        return new ConnectClusterDO()
                .setId(this.id)
                .setConnectRestApiUrl(this.connectRestApiUrl)
                .setDescription(this.description)
                .setVersion(this.version);
    }
}
