package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

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
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "project")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ProjectDO extends SoftDeletableDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "名称", required = true)
    @Column(name = "name", length = 128, nullable = false)
    private String name;

    @ApiModelProperty("描述")
    @Column(name = "description", length = 1024)
    private String description;

    @ApiModelProperty("项目拥有者")
    @ManyToOne
    private UserDO owner;

    @ApiModelProperty("数据来源")
    @Column(name = "source")
    @Enumerated(EnumType.ORDINAL)
    private MetadataSourceType source;

    @ApiModelProperty("原始id")
    @Column(name = "original_id")
    private Long originalId;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "rel_project__user",
            joinColumns = @JoinColumn(name = "project_id"),
            inverseJoinColumns = @JoinColumn(name = "user_id")
    )
    private Set<UserDO> users = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY, mappedBy = "projects")
    private Set<DataSystemResourceDO> dataSystemResources = new HashSet<>();

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "sourceProject")
    private Set<ConnectionDO> connectionsWithThisAsSourceProject = new HashSet<>();

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "sinkProject")
    private Set<ConnectionDO> connectionsWithThisAsSinkProject = new HashSet<>();

    public ProjectDO(final Long id) {
        this.id = id;
    }

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public ProjectDO addUser(UserDO user) {
        this.users.add(user);
        return this;
    }

    public ProjectDO removeUser(UserDO user) {
        this.users.remove(user);
        return this;
    }

    // functions for jpa union features
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProjectDO)) {
            return false;
        }
        return id != null && id.equals(((ProjectDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Project{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", description='" + getDescription() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
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
