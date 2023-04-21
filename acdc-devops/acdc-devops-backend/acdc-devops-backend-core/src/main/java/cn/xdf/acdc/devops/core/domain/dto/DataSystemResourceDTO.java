package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Accessors(chain = true)
public class DataSystemResourceDTO {

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

    @Builder.Default
    private Boolean deleted = Boolean.FALSE;

    private Date creationTime;

    private Date updateTIme;

    public DataSystemResourceDTO(final DataSystemResourceDO dataSystemResource) {
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
    }
}
