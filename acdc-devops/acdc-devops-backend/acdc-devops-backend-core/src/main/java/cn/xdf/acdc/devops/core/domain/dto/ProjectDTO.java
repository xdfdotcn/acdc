package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Objects;

// CHECKSTYLE:OFF
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProjectDTO extends PageDTO {

    private Long id;

    private String name;

    private String description;

    private Long owner;

    private String ownerEmail;

    private ProjectSourceType source;

    private Long originalId;

    private Instant creationTime;

    public ProjectDTO(final ProjectDO project) {
        this.id = project.getId();
        this.name = project.getName();
        this.description = project.getDescription();
        this.ownerEmail = Objects.nonNull(project.getOwner()) ? project.getOwner().getEmail() : SystemConstant.EMPTY_STRING;
    }

    public static ProjectDTO toProjectDTO(final ProjectDO project) {
        ProjectDTOBuilder builder = ProjectDTO.builder()
            .id(project.getId())
            .name(project.getName())
            .description(project.getDescription())
            .creationTime(project.getCreationTime());
        if (Objects.nonNull(project.getOwner())) {
            builder.ownerEmail = project.getOwner().getEmail();
        }
        return builder.build();
    }

    public static ProjectDO toProjectDO(final ProjectDTO projectDTO) {
        ProjectDO projectDO = new ProjectDO();
        projectDO.setName(projectDTO.getName());
        projectDO.setId(projectDTO.getId());
        projectDO.setDescription(projectDTO.getDescription());
        projectDO.setSource(projectDTO.getSource());
        projectDO.setOriginalId(projectDTO.getOriginalId());
        return projectDO;
    }
}
