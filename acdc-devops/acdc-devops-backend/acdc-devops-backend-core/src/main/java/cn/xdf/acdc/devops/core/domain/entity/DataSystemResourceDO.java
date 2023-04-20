package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "data_system_resource")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@SuperBuilder
@Accessors(chain = true)
public class DataSystemResourceDO extends SoftDeletableDO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty("父资源")
    @ManyToOne(fetch = FetchType.LAZY)
    private DataSystemResourceDO parentResource;

    @ApiModelProperty(value = "名称", required = true)
    @Column(name = "name", length = 256, nullable = false)
    private String name;

    @Column(name = "data_system_type", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private DataSystemType dataSystemType;

    @Column(name = "resource_type", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private DataSystemResourceType resourceType;

    @ApiModelProperty("描述")
    @Column(name = "description", length = 1024)
    private String description;

    @Builder.Default
    @OneToMany(mappedBy = "dataSystemResource", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private Set<DataSystemResourceConfigurationDO> dataSystemResourceConfigurations = new HashSet<>();

    @Builder.Default
    @OneToMany(mappedBy = "parentResource", fetch = FetchType.LAZY)
    private Set<DataSystemResourceDO> childrenResources = new HashSet<>();

    @Builder.Default
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "project_data_system_resource_mapping",
            joinColumns = @JoinColumn(name = "data_system_resource_id"),
            inverseJoinColumns = @JoinColumn(name = "project_id")
    )
    private Set<ProjectDO> projects = new HashSet<>();

    @Builder.Default
    @ApiModelProperty("与本 data system resource 有关联的 connectors")
    @OneToMany(mappedBy = "dataSystemResource")
    private Set<ConnectorDO> connectors = new HashSet<>();

    @OneToOne(fetch = FetchType.LAZY, mappedBy = "dataSystemResource")
    private KafkaTopicDO kafkaTopic;

    @Builder.Default
    @OneToMany(fetch = FetchType.LAZY, mappedBy = "sourceDataCollection")
    private Set<ConnectionDO> connectionsWithThisAsSourceDataCollection = new HashSet<>();

    @Builder.Default
    @OneToMany(fetch = FetchType.LAZY, mappedBy = "sinkDataCollection")
    private Set<ConnectionDO> connectionsWithThisAsSinkDataCollection = new HashSet<>();

    @Builder.Default
    @OneToMany(fetch = FetchType.LAZY, mappedBy = "sinkInstance")
    private Set<ConnectionDO> connectionsWithThisAsSinkInstance = new HashSet<>();

    public DataSystemResourceDO(final Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataSystemResourceDO)) {
            return false;
        }
        return id != null && id.equals(((DataSystemResourceDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    /**
     * A pojo signature.
     *
     * @return signature
     */
    public String getSignature() {
        return id + "," + name;
    }
}
