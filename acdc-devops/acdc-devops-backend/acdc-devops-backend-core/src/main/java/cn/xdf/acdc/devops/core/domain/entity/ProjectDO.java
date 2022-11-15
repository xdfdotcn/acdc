package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "project")
@Data
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
    private ProjectSourceType source;

    @ApiModelProperty("原始id")
    @Column(name = "original_id")
    private Long originalId;

    @ManyToMany
    @JoinTable(name = "rel_project__rdb", joinColumns = @JoinColumn(name = "project_id"), inverseJoinColumns = @JoinColumn(name = "rdb_id"))
    @JsonIgnoreProperties(value = {"rdbDatabases", "rdbInstances", "kafkaCluster", "projects"}, allowSetters = true)
    private Set<RdbDO> rdbs = new HashSet<>();

    @ManyToMany
    @JoinTable(
            name = "rel_project__user",
            joinColumns = @JoinColumn(name = "project_id"),
            inverseJoinColumns = @JoinColumn(name = "user_id")
    )
    private Set<UserDO> users = new HashSet<>();

    @ManyToMany(mappedBy = "projects")
    @JsonIgnoreProperties(value = {"hiveDatabases", "hdfs", "projects"}, allowSetters = true)
    private Set<HiveDO> hives = new HashSet<>();

    @ManyToMany
    @JoinTable(
            name = "kafka_cluster_project_mapping",
            joinColumns = @JoinColumn(name = "project_id"),
            inverseJoinColumns = @JoinColumn(name = "kafka_cluster_id")
    )
    private Set<KafkaClusterDO> kafkaClusters = new HashSet<>();

    public ProjectDO(final Long id) {
        this.id = id;
    }

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public void setHives(Set<HiveDO> hives) {
        if (this.hives != null) {
            this.hives.forEach(i -> i.removeProject(this));
        }
        if (hives != null) {
            hives.forEach(i -> i.addProject(this));
        }
        this.hives = hives;
    }

    public ProjectDO addUser(UserDO user) {
        this.users.add(user);
        return this;
    }

    public ProjectDO removeUser(UserDO user) {
        this.users.remove(user);
        return this;
    }

    public ProjectDO addRdb(RdbDO rdb) {
        this.rdbs.add(rdb);
        rdb.getProjects().add(this);
        return this;
    }

    public ProjectDO removeRdb(RdbDO rdb) {
        this.rdbs.remove(rdb);
        rdb.getProjects().remove(this);
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
