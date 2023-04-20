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

    private Long parentResourceId;

    private String parentResourceName;

    private DataSystemResourceType parentResourceType;

    private Long kafkaTopicId;

    private String kafkaTopicName;

    private Boolean deleted = Boolean.FALSE;

    private Date creationTime;

    private Date updateTIme;

    private Map<String, DataSystemResourceConfigurationDTO> dataSystemResourceConfigurations = new HashMap();

    private List<ProjectDTO> projects = new ArrayList();

    public DataSystemResourceDetailDTO(final DataSystemResourceDO dataSystemResource) {
        this.getDataSystemResourceConfigurations();
        this.id = dataSystemResource.getId();
        this.name = dataSystemResource.getName();
        this.dataSystemType = dataSystemResource.getDataSystemType();
        this.resourceType = dataSystemResource.getResourceType();
        this.description = dataSystemResource.getDescription();

        if (Objects.nonNull(dataSystemResource.getParentResource())) {
            this.parentResourceId = dataSystemResource.getParentResource().getId();
            this.parentResourceName = dataSystemResource.getParentResource().getName();
            this.parentResourceType = dataSystemResource.getParentResource().getResourceType();
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
     * Convert to DO.
     *
     * @return DataSystemResourceDO
     */
    public DataSystemResourceDO toDO() {
        DataSystemResourceDO parent = null;
        if (Objects.nonNull(parentResourceId)) {
            parent = DataSystemResourceDO.builder()
                    .id(parentResourceId)
                    .name(parentResourceName)
                    .resourceType(parentResourceType)
                    .build();
        }

        KafkaTopicDO kafkaTopic = null;
        if (Objects.nonNull(kafkaTopicId)) {
            kafkaTopic = KafkaTopicDO.builder()
                    .id(kafkaTopicId)
                    .name(kafkaTopicName)
                    .build();
        }

        Set<ProjectDO> projects = this.projects.stream().map(ProjectDTO::toDO).collect(Collectors.toSet());

        Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = this.dataSystemResourceConfigurations.values()
                .stream()
                .map(DataSystemResourceConfigurationDTO::toDO)
                .collect(Collectors.toSet());

        DataSystemResourceDO dataSystemResource = DataSystemResourceDO.builder()
                .id(id)
                .name(name)
                .dataSystemType(dataSystemType)
                .resourceType(resourceType)
                .description(description)
                .parentResource(parent)
                .kafkaTopic(kafkaTopic)
                .projects(projects)
                .dataSystemResourceConfigurations(dataSystemResourceConfigurations)
                .deleted(deleted)
                .creationTime(this.creationTime)
                .updateTime(this.updateTIme)
                .build();

        // for cascade save
        dataSystemResourceConfigurations.forEach(each -> each.setDataSystemResource(dataSystemResource));
        return dataSystemResource;
    }
}
