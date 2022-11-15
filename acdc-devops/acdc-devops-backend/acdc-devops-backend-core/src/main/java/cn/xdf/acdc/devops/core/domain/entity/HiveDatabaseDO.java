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
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "Hive database 信息.\n@author acdc")
@Entity
@Table(name = "hive_database")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class HiveDatabaseDO extends SoftDeletableDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "库名", required = true)
    @Column(name = "name", length = 200, nullable = false)
    private String name;

    @ApiModelProperty("hive数据表")
    @OneToMany(mappedBy = "hiveDatabase")
    @JsonIgnoreProperties(value = {"sinkHiveTable", "hiveDatabase"}, allowSetters = true)
    private Set<HiveTableDO> hiveTables = new HashSet<>();

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"hiveDatabases", "hdfs", "projects"}, allowSetters = true)
    private HiveDO hive;

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public void setHiveTables(Set<HiveTableDO> hiveTables) {
        if (this.hiveTables != null) {
            this.hiveTables.forEach(i -> i.setHiveDatabase(null));
        }
        if (hiveTables != null) {
            hiveTables.forEach(i -> i.setHiveDatabase(this));
        }
        this.hiveTables = hiveTables;
    }

    public HiveDatabaseDO addHiveTable(HiveTableDO hiveTable) {
        this.hiveTables.add(hiveTable);
        hiveTable.setHiveDatabase(this);
        return this;
    }

    public HiveDatabaseDO removeHiveTable(HiveTableDO hiveTable) {
        this.hiveTables.remove(hiveTable);
        hiveTable.setHiveDatabase(null);
        return this;
    }

    // functions for jpa union feature
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveDatabaseDO)) {
            return false;
        }
        return id != null && id.equals(((HiveDatabaseDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "HiveDatabase{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
