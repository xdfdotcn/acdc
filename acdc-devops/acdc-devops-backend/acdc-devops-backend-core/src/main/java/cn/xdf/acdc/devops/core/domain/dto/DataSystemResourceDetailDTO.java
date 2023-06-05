package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class DataSystemResourceDetailDTO {
    
    private Long id;
    
    private String name;
    
    private DataSystemType dataSystemType;
    
    private DataSystemResourceType resourceType;
    
    private String description;
    
    private DataSystemResourceDetailDTO parentResource;
    
    private Long kafkaTopicId;
    
    private String kafkaTopicName;
    
    private Boolean deleted = Boolean.FALSE;
    
    private Date creationTime;
    
    private Date updateTIme;
    
    private Map<String, DataSystemResourceConfigurationDTO> dataSystemResourceConfigurations = new HashMap<>();
    
    private List<ProjectDTO> projects = new ArrayList<>();
    
    public DataSystemResourceDetailDTO(final DataSystemResourceDO dataSystemResource) {
        this.id = dataSystemResource.getId();
        this.name = dataSystemResource.getName();
        this.dataSystemType = dataSystemResource.getDataSystemType();
        this.resourceType = dataSystemResource.getResourceType();
        this.description = dataSystemResource.getDescription();
        
        if (Objects.nonNull(dataSystemResource.getParentResource())) {
            this.parentResource = new DataSystemResourceDetailDTO(dataSystemResource.getParentResource());
        }
        
        if (Objects.nonNull(dataSystemResource.getKafkaTopic())) {
            this.kafkaTopicId = dataSystemResource.getKafkaTopic().getId();
            this.kafkaTopicName = dataSystemResource.getKafkaTopic().getName();
        }
        
        this.deleted = dataSystemResource.getDeleted();
        this.creationTime = dataSystemResource.getCreationTime();
        this.updateTIme = dataSystemResource.getUpdateTime();
        
        dataSystemResource.getDataSystemResourceConfigurations().forEach(each ->
                dataSystemResourceConfigurations.put(each.getName(), new DataSystemResourceConfigurationDTO(each)));
        
        dataSystemResource.getProjects().forEach(each -> projects.add(new ProjectDTO(each)));
    }
    
    /**
     * Only use for deliver the relation of parent when saving data system resource.
     *
     * @param id id of resource
     */
    public DataSystemResourceDetailDTO(final Long id) {
        this.id = id;
    }
    
    /**
     * Convert to DO.
     *
     * @return DataSystemResourceDO
     */
    public DataSystemResourceDO toDO() {
        DataSystemResourceDO parent = null;
        if (Objects.nonNull(parentResource)) {
            parent = parentResource.toDO();
        }
        
        KafkaTopicDO kafkaTopic = null;
        if (Objects.nonNull(kafkaTopicId)) {
            kafkaTopic = new KafkaTopicDO()
                    .setId(kafkaTopicId)
                    .setName(kafkaTopicName);
        }
        
        Set<ProjectDO> projects = this.projects.stream().map(ProjectDTO::toDO).collect(Collectors.toSet());
        
        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = this.dataSystemResourceConfigurations.values()
                .stream()
                .map(DataSystemResourceConfigurationDTO::toDO)
                .collect(Collectors.toSet());
        
        DataSystemResourceDO dataSystemResource = new DataSystemResourceDO();
        dataSystemResource.setId(id);
        dataSystemResource.setName(name);
        dataSystemResource.setDataSystemType(dataSystemType);
        dataSystemResource.setResourceType(resourceType);
        dataSystemResource.setDescription(description);
        dataSystemResource.setParentResource(parent);
        dataSystemResource.setKafkaTopic(kafkaTopic);
        dataSystemResource.setProjects(projects);
        dataSystemResource.setDataSystemResourceConfigurations(dataSystemResourceConfigurations);
        dataSystemResource.setDeleted(deleted);
        dataSystemResource.setCreationTime(this.creationTime);
        dataSystemResource.setUpdateTime(this.updateTIme);
        
        // for cascade save
        dataSystemResourceConfigurations.forEach(each -> each.setDataSystemResource(dataSystemResource));
        return dataSystemResource;
    }
}
