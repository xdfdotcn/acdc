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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "connector数据扩展，暂时都是string类型，或者隐含的一些关键字类型，例如${datetime}")
@Entity
@Table(name = "connector_data_extension")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ConnectorDataExtensionDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "字段名称", required = true)
    @Column(name = "name", length = 128, nullable = false)
    private String name;

    @ApiModelProperty(value = "字段类型", required = true)
    @Column(name = "value", length = 1024, nullable = false)
    private String value;

    @ManyToMany(mappedBy = "connectorDataExtensions")
    @JsonIgnoreProperties(value = {"connector", "rdbTable", "kafkaTopic", "connectorDataExtensions"}, allowSetters = true)
    private Set<SourceRdbTableDO> sourceRdbTables = new HashSet<>();

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public void setSourceRdbTables(Set<SourceRdbTableDO> sourceRdbTables) {
        if (this.sourceRdbTables != null) {
            this.sourceRdbTables.forEach(i -> i.removeConnectorDataExtension(this));
        }
        if (sourceRdbTables != null) {
            sourceRdbTables.forEach(i -> i.addConnectorDataExtension(this));
        }
        this.sourceRdbTables = sourceRdbTables;
    }

    public ConnectorDataExtensionDO addSourceRdbTable(SourceRdbTableDO sourceRdbTable) {
        this.sourceRdbTables.add(sourceRdbTable);
        sourceRdbTable.getConnectorDataExtensions().add(this);
        return this;
    }

    public ConnectorDataExtensionDO removeSourceRdbTable(SourceRdbTableDO sourceRdbTable) {
        this.sourceRdbTables.remove(sourceRdbTable);
        sourceRdbTable.getConnectorDataExtensions().remove(this);
        return this;
    }

    // functions for jpa union features
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorDataExtensionDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectorDataExtensionDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "ConnectorDataExtension{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", value='" + getValue() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
