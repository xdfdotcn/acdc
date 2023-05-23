package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.NotFound;
import org.hibernate.annotations.NotFoundAction;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "connector实例")
@Entity
@Table(name = "connector")
@DynamicUpdate
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectorDO extends BaseDO implements Serializable {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty(value = "connector名称", required = true)
    @Column(name = "name", length = 128, nullable = false, unique = true)
    private String name;
    
    @OneToMany(mappedBy = "connector", cascade = CascadeType.ALL)
    @JsonIgnoreProperties(value = "connector", allowSetters = true)
    @NotFound(action = NotFoundAction.IGNORE)
    private Set<ConnectorConfigurationDO> connectorConfigurations = new HashSet<>();
    
    @ApiModelProperty("connector实现类")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"defaultConnectorConfigurations", "connectorType", "dimServiceType"}, allowSetters = true)
    private ConnectorClassDO connectorClass;
    
    @ApiModelProperty("connect集群")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"connectorClass", "connectors"}, allowSetters = true)
    private ConnectClusterDO connectCluster;
    
    @ApiModelProperty("kafka集群")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"kafkaTopics", "connectors"}, allowSetters = true)
    private KafkaClusterDO kafkaCluster;
    
    @ApiModelProperty("本 connector 对应的 data system resource")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private DataSystemResourceDO dataSystemResource;
    
    @ApiModelProperty("desired state")
    @Enumerated(EnumType.ORDINAL)
    @Column(name = "desired_state")
    private ConnectorState desiredState;
    
    @ApiModelProperty("actual state")
    @Column(name = "actual_state")
    @Enumerated(EnumType.ORDINAL)
    private ConnectorState actualState;
    
    @OneToMany(fetch = FetchType.LAZY, mappedBy = "sourceConnector")
    private Set<ConnectionDO> connectionsWithThisAsSourceConnector = new HashSet<>();
    
    @OneToOne(fetch = FetchType.LAZY, mappedBy = "sinkConnector")
    private ConnectionDO connectionWithThisAsSinkConnector;
    
    public ConnectorDO(final Long id) {
        this.id = id;
    }
    
    // functions for jpa union features
    // CHECKSTYLE:OFF
    
    public ConnectorDO setConnectorConfigurations(Set<ConnectorConfigurationDO> connectorConfigurations) {
        if (this.connectorConfigurations != null) {
            this.connectorConfigurations.forEach(i -> i.setConnector(null));
        }
        if (connectorConfigurations != null) {
            connectorConfigurations.forEach(i -> i.setConnector(this));
        }
        this.connectorConfigurations = connectorConfigurations;
        return this;
    }
    
    // functions for jpa union features
    // CHECKSTYLE:ON
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectorDO) o).id);
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
    
    // prettier-ignore
    @Override
    public String toString() {
        return "Connector{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
