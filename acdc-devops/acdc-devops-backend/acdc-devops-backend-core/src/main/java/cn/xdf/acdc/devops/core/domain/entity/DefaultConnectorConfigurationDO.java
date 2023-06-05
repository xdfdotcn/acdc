package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@ApiModel(description = "默认的配置项，例如schema注册中心地址，序列化方式等")
@Entity
@Table(name = "default_connector_config")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class DefaultConnectorConfigurationDO extends BaseDO implements Serializable {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty(value = "配置项名称", required = true)
    @Column(name = "name", length = 128, nullable = false)
    private String name;
    
    @ApiModelProperty(value = "配置项值", required = true)
    @Column(name = "value", length = 1024, nullable = false)
    private String value;
    
    @ApiModelProperty("connector实现类")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"defaultConnectorConfigurations", "connectorType", "dimServiceType"}, allowSetters = true)
    private ConnectorClassDO connectorClass;
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultConnectorConfigurationDO)) {
            return false;
        }
        return id != null && id.equals(((DefaultConnectorConfigurationDO) o).id);
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
    
    // prettier-ignore
    @Override
    public String toString() {
        return "DefaultConnectorConfiguration{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", value='" + getValue() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
