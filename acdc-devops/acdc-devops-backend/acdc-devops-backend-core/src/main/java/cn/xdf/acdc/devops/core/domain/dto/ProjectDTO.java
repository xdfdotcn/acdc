package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.Objects;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class ProjectDTO {

    private Long id;

    private String name;

    private String description;

    private Long ownerId;

    private String ownerEmail;

    private String ownerName;

    private MetadataSourceType source;

    private Long originalId;

    private Date creationTime;

    private Date updateTime;

    public ProjectDTO(final ProjectDO project) {
        this.id = project.getId();
        this.name = project.getName();
        this.description = project.getDescription();

        if (Objects.nonNull(project.getOwner())) {
            this.ownerId = project.getOwner().getId();
        }

        this.ownerEmail = Objects.nonNull(project.getOwner()) ? project.getOwner().getEmail() : SystemConstant.EMPTY_STRING;
        this.ownerName = Objects.nonNull(project.getOwner()) ? project.getOwner().getName() : SystemConstant.EMPTY_STRING;
        this.source = project.getSource();
        this.originalId = project.getOriginalId();
        this.creationTime = project.getCreationTime();
        this.updateTime = project.getUpdateTime();
    }

    public ProjectDTO(final Long id) {
        this.id = id;
    }

    /**
     * Convert to ProjectDO.
     *
     * @return ProjectDO
     */
    public ProjectDO toDO() {
        ProjectDO projectDO = new ProjectDO();
        projectDO.setName(this.getName());
        projectDO.setId(this.getId());
        projectDO.setOwner(new UserDO(this.ownerId));
        projectDO.setDescription(this.getDescription());
        projectDO.setSource(this.getSource());
        projectDO.setOriginalId(this.getOriginalId());
        return projectDO;
    }
}
