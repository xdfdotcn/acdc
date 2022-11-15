package cn.xdf.acdc.devops.core.domain.entity;

// CHECKSTYLE:OFF

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
import javax.persistence.Table;
import java.io.Serializable;

/**
 * A RdbTable.
 */
@Entity
@Table(name = "rdb_table")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class RdbTableDO extends SoftDeletableDO implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "表名", required = true)
    @Column(name = "name", length = 128, nullable = false)
    private String name;

    @ApiModelProperty(value = "所属DB")
    @ManyToOne
    @JsonIgnoreProperties(value = {"rdbTables", "rdb"}, allowSetters = true)
    private RdbDatabaseDO rdbDatabase;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RdbTableDO)) {
            return false;
        }
        return id != null && id.equals(((RdbTableDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "RdbTable{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
