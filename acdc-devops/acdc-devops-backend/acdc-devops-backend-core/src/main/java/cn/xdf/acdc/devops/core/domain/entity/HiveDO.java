package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModel;
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
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "Hive集集群信息.\n@author acdc")
@Entity
@Table(name = "hive")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class HiveDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "集群名称", required = true)
    @Column(name = "name", length = 32, nullable = false)
    private String name;

    @ApiModelProperty("metastore uri")
    @Column(name = "metastore_uris", length = 500)
    private String metastoreUris;

    @Column(name = "hdfs_user", length = 32, nullable = false)
    private String hdfsUser;

    @ApiModelProperty("hive 数据库")
    @OneToMany(mappedBy = "hive")
    @JsonIgnoreProperties(value = {"hiveTables", "hive"}, allowSetters = true)
    private Set<HiveDatabaseDO> hiveDatabases = new HashSet<>();

    @ApiModelProperty("hdfs集群")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"hdfsNamenodes", "hives"}, allowSetters = true)
    private HdfsDO hdfs;

    @ManyToMany
    @JoinTable(
            name = "rel_hive__project",
            joinColumns = @JoinColumn(name = "hive_id"),
            inverseJoinColumns = @JoinColumn(name = "project_id")
    )
    @JsonIgnoreProperties(value = {"owner", "rdbs", "users", "hives"}, allowSetters = true)
    private Set<ProjectDO> projects = new HashSet<>();

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public void setHiveDatabases(Set<HiveDatabaseDO> hiveDatabases) {
        if (this.hiveDatabases != null) {
            this.hiveDatabases.forEach(i -> i.setHive(null));
        }
        if (hiveDatabases != null) {
            hiveDatabases.forEach(i -> i.setHive(this));
        }
        this.hiveDatabases = hiveDatabases;
    }

    public HiveDO addHiveDatabase(HiveDatabaseDO hiveDatabase) {
        this.hiveDatabases.add(hiveDatabase);
        hiveDatabase.setHive(this);
        return this;
    }

    public HiveDO removeHiveDatabase(HiveDatabaseDO hiveDatabase) {
        this.hiveDatabases.remove(hiveDatabase);
        hiveDatabase.setHive(null);
        return this;
    }

    public HiveDO addProject(ProjectDO project) {
        this.projects.add(project);
        project.getHives().add(this);
        return this;
    }

    public HiveDO removeProject(ProjectDO project) {
        this.projects.remove(project);
        project.getHives().remove(this);
        return this;
    }

    // functions for jpa union features
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveDO)) {
            return false;
        }
        return id != null && id.equals(((HiveDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Hive{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", metastoreUris='" + getMetastoreUris() + "'"
                + ", hdfsUser='" + getHdfsUser() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
