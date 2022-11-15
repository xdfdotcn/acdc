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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "rdb_database")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class RdbDatabaseDO extends SoftDeletableDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "数据库名称", required = true)
    @Column(name = "name", length = 128, nullable = false)
    private String name;

    @OneToMany(mappedBy = "rdbDatabase")
    @JsonIgnoreProperties(value = {"rdbDatabase"}, allowSetters = true)
    private Set<RdbTableDO> rdbTables = new HashSet<>();

    @ApiModelProperty("所属数据库集群服务")
    @ManyToOne
    @JsonIgnoreProperties(value = {"rdbDatabases", "rdbInstances", "kafkaCluster", "projects"}, allowSetters = true)
    private RdbDO rdb;

    // functions for jpa union feature
    // CHECKSTYLE:OFF

    public void setRdbTables(Set<RdbTableDO> rdbTables) {
        if (this.rdbTables != null) {
            this.rdbTables.forEach(i -> i.setRdbDatabase(null));
        }
        if (rdbTables != null) {
            rdbTables.forEach(i -> i.setRdbDatabase(this));
        }
        this.rdbTables = rdbTables;
    }

    public RdbDatabaseDO addRdbTable(RdbTableDO rdbTable) {
        this.rdbTables.add(rdbTable);
        rdbTable.setRdbDatabase(this);
        return this;
    }

    public RdbDatabaseDO removeRdbTable(RdbTableDO rdbTable) {
        this.rdbTables.remove(rdbTable);
        rdbTable.setRdbDatabase(null);
        return this;
    }

    // functions for jpa union feature
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RdbDatabaseDO)) {
            return false;
        }
        return name != null && name.equals(((RdbDatabaseDO) o).name);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "RdbDatabase{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
