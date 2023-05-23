package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class DataSystemResourceConfigurationDTO {
    
    private Long id;
    
    private Long dataSystemResourceId;
    
    private String name;
    
    private String value;
    
    private Date creationTime;
    
    private Date updateTIme;
    
    public DataSystemResourceConfigurationDTO(final DataSystemResourceConfigurationDO dataSystemResourceConfiguration) {
        this.id = dataSystemResourceConfiguration.getId();
        this.dataSystemResourceId = dataSystemResourceConfiguration.getDataSystemResource().getId();
        this.name = dataSystemResourceConfiguration.getName();
        this.value = dataSystemResourceConfiguration.getValue();
        this.creationTime = dataSystemResourceConfiguration.getCreationTime();
        this.updateTIme = dataSystemResourceConfiguration.getUpdateTime();
    }
    
    /**
     * To DataSystemResourceConfigurationDO.
     *
     * @return DataSystemResourceConfigurationDO
     */
    public DataSystemResourceConfigurationDO toDO() {
        DataSystemResourceDO dataSystemResourceDO = new DataSystemResourceDO().setId(this.dataSystemResourceId);
        
        DataSystemResourceConfigurationDO result = new DataSystemResourceConfigurationDO();
        result.setId(this.id);
        result.setName(this.name);
        result.setValue(this.value);
        result.setDataSystemResource(dataSystemResourceDO);
        result.setCreationTime(this.creationTime);
        result.setUpdateTime(this.updateTIme);
        return result;
    }
}
