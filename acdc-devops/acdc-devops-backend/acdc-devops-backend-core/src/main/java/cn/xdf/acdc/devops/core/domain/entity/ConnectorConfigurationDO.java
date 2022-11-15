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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "connector_configuration")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ConnectorConfigurationDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "配置项名称", required = true)
    @Column(name = "name", length = 200, nullable = false)
    private String name;

    @ApiModelProperty(value = "配置项值", required = true)
    @Lob
    @Column(name = "value", nullable = false)
    private String value;

    @ApiModelProperty("所属connector")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(
            value = {"connectorConfigurations", "creator", "connectorClass", "connectCluster", "kafkaCluster",
                    "desiredState", "actualState"},
            allowSetters = true
    )
    private ConnectorDO connector;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorConfigurationDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectorConfigurationDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "ConnectorConfiguration{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", value='" + getValue() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
