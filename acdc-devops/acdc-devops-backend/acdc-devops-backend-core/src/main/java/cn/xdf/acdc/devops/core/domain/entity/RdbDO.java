package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Entity
@Table(name = "rdb")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class RdbDO extends SoftDeletableDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "类型", required = true)
    @Column(name = "rdb_type", length = 32, nullable = false)
    private String rdbType;

    @ApiModelProperty(value = "数据库集群名称", required = true)
    @Column(name = "name", length = 128, nullable = false)
    private String name;

    @ApiModelProperty(value = "用户名", required = true)
    @Column(name = "username", length = 1024, nullable = false)
    private String username;

    @ApiModelProperty(value = "密码", required = true)
    @Column(name = "password", length = 1024, nullable = false)
    private String password;

    @ApiModelProperty("描述")
    @Column(name = "description", length = 1024)
    private String description;

    @OneToMany(mappedBy = "rdb", fetch = FetchType.EAGER)
    @JsonIgnoreProperties(value = {"rdbTables", "rdb"}, allowSetters = true)
    private Set<RdbDatabaseDO> rdbDatabases = new HashSet<>();

    @OneToMany(mappedBy = "rdb", fetch = FetchType.EAGER)
    @JsonIgnoreProperties(value = {"role", "rdb"}, allowSetters = true)
    private Set<RdbInstanceDO> rdbInstances = new HashSet<>();

    @ManyToMany(mappedBy = "rdbs")
    @JsonIgnoreProperties(value = {"owner", "rdbs", "users", "hives"}, allowSetters = true)
    private Set<ProjectDO> projects = new HashSet<>();

    // functions for jpa union feature
    // CHECKSTYLE:OFF

    public void setRdbDatabases(Set<RdbDatabaseDO> rdbDatabases) {
        if (this.rdbDatabases != null) {
            this.rdbDatabases.forEach(i -> i.setRdb(null));
        }
        if (rdbDatabases != null) {
            rdbDatabases.forEach(i -> i.setRdb(this));
        }
        this.rdbDatabases = rdbDatabases;
    }

    public RdbDO addRdbDatabase(RdbDatabaseDO rdbDatabase) {
        this.rdbDatabases.add(rdbDatabase);
        rdbDatabase.setRdb(this);
        return this;
    }

    public RdbDO removeRdbDatabase(RdbDatabaseDO rdbDatabase) {
        this.rdbDatabases.remove(rdbDatabase);
        rdbDatabase.setRdb(null);
        return this;
    }

    public void setRdbInstances(Set<RdbInstanceDO> rdbInstances) {
        if (this.rdbInstances != null) {
            this.rdbInstances.forEach(i -> i.setRdb(null));
        }
        if (rdbInstances != null) {
            rdbInstances.forEach(i -> i.setRdb(this));
        }
        this.rdbInstances = rdbInstances;
    }

    public RdbDO addRdbInstance(RdbInstanceDO rdbInstance) {
        this.rdbInstances.add(rdbInstance);
        rdbInstance.setRdb(this);
        return this;
    }

    public RdbDO removeRdbInstance(RdbInstanceDO rdbInstance) {
        this.rdbInstances.remove(rdbInstance);
        rdbInstance.setRdb(null);
        return this;
    }

    public void setProjects(Set<ProjectDO> projects) {
        if (this.projects != null) {
            this.projects.forEach(i -> i.removeRdb(this));
        }
        if (projects != null) {
            projects.forEach(i -> i.addRdb(this));
        }
        this.projects = projects;
    }

    public RdbDO addProject(ProjectDO project) {
        this.projects.add(project);
        project.getRdbs().add(this);
        return this;
    }

    public RdbDO removeProject(ProjectDO project) {
        this.projects.remove(project);
        project.getRdbs().remove(this);
        return this;
    }

    // CHECKSTYLE:ON
    // functions for jpa union feature

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RdbDO)) {
            return false;
        }
        return id != null && id.equals(((RdbDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Rdb{"
                + "id=" + getId()
                + ", rdbType='" + getRdbType() + "'"
                + ", name='" + getName() + "'"
                + ", username='" + getUsername() + "'"
                + ", password='" + getPassword() + "'"
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

    /**
     * Get db instance list.
     *
     * @return db instance list
     */
    public List<String> getDbInstances() {
        if (Objects.isNull(rdbInstances)) {
            return new ArrayList<>();
        }
        return rdbInstances.stream().map(RdbInstanceDO::getSignature).collect(Collectors.toList());
    }
}
