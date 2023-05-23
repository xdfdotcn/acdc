package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "connector的java实现类")
@Entity
@Table(name = "connector_class")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectorClassDO extends BaseDO implements Serializable {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty(value = "全限类名", required = true)
    @Column(name = "name", length = 1024, nullable = false)
    private String name;
    
    @ApiModelProperty(value = "非全限类名", required = true)
    @Column(name = "simple_name", length = 128, nullable = false)
    private String simpleName;
    
    @ApiModelProperty("描述")
    @Column(name = "description", length = 1024)
    private String description;
    
    @OneToMany(mappedBy = "connectorClass")
    @JsonIgnoreProperties(value = "connectorClass", allowSetters = true)
    private Set<DefaultConnectorConfigurationDO> defaultConnectorConfigurations = new HashSet<>();
    
    @OneToMany(mappedBy = "connectorClass")
    private Set<ConnectClusterDO> connectClusters = new HashSet<>();
    
    @ApiModelProperty("connector 类型")
    @Enumerated(EnumType.ORDINAL)
    private ConnectorType connectorType;
    
    @ApiModelProperty("数据系统类型")
    @Enumerated(EnumType.ORDINAL)
    private DataSystemType dataSystemType;
    
    public ConnectorClassDO(final Long id) {
        this.id = id;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorClassDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectorClassDO) o).id);
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
    
    // prettier-ignore
    @Override
    public String toString() {
        return "ConnectorClass{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", simpleName='" + getSimpleName() + "'"
                + ", description='" + getDescription() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
