package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class DataSystemResourceDTO {
    
    private Long id;
    
    private String name;
    
    private DataSystemType dataSystemType;
    
    private DataSystemResourceType resourceType;
    
    private String description;
    
    private DataSystemResourceDTO parentResource;
    
    private Long kafkaTopicId;
    
    private String kafkaTopicName;
    
    private Boolean deleted = Boolean.FALSE;
    
    private Date creationTime;
    
    private Date updateTime;
    
    public DataSystemResourceDTO(final DataSystemResourceDO dataSystemResource) {
        this.id = dataSystemResource.getId();
        this.name = dataSystemResource.getName();
        this.dataSystemType = dataSystemResource.getDataSystemType();
        this.resourceType = dataSystemResource.getResourceType();
        this.description = dataSystemResource.getDescription();
        
        if (Objects.nonNull(dataSystemResource.getParentResource())) {
            this.parentResource = new DataSystemResourceDTO(dataSystemResource.getParentResource());
        }
        
        if (Objects.nonNull(dataSystemResource.getKafkaTopic())) {
            this.kafkaTopicId = dataSystemResource.getKafkaTopic().getId();
            this.kafkaTopicName = dataSystemResource.getKafkaTopic().getName();
        }
        
        this.deleted = dataSystemResource.getDeleted();
        this.creationTime = dataSystemResource.getCreationTime();
        this.updateTime = dataSystemResource.getUpdateTime();
    }
    
    /**
     * To DataSystemResourceDO.
     *
     * @return DataSystemResourceDO
     */
    public DataSystemResourceDO toDO() {
        return new DataSystemResourceDO().setId(this.id);
    }
}
