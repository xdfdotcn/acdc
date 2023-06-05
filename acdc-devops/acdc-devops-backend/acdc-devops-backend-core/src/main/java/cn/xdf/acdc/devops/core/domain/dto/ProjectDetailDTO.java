package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class ProjectDetailDTO {
    
    private Long id;
    
    private String name;
    
    private String description;
    
    private Long ownerId;
    
    private MetadataSourceType source;
    
    private Long originalId;
    
    private Set<Long> userIds = new HashSet<>();
    
    public ProjectDetailDTO(final ProjectDO project) {
        this.id = project.getId();
        this.name = project.getName();
        this.description = project.getDescription();
        
        if (Objects.nonNull(project.getOwner())) {
            this.ownerId = project.getOwner().getId();
        }
        
        this.source = project.getSource();
        this.originalId = project.getOriginalId();
        
        if (Objects.nonNull(project.getUsers())) {
            this.userIds = project.getUsers().stream().map(UserDO::getId).collect(Collectors.toSet());
        }
    }
    
    /**
     * Convert to DO.
     *
     * @return ProjectDO
     */
    public ProjectDO toDO() {
        ProjectDO projectDO = new ProjectDO();
        projectDO.setId(this.getId());
        projectDO.setName(this.getName());
        projectDO.setDescription(this.getDescription());
        if (Objects.nonNull(this.ownerId)) {
            projectDO.setOwner(new UserDO(ownerId));
        }
        projectDO.setSource(this.getSource());
        projectDO.setOriginalId(this.getOriginalId());
        if (Objects.nonNull(this.userIds)) {
            projectDO.setUsers(userIds.stream().map(UserDO::new).collect(Collectors.toSet()));
        }
        return projectDO;
    }
}
