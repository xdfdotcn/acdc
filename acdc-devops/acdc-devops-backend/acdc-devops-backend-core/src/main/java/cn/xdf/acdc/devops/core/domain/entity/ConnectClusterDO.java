package cn.xdf.acdc.devops.core.domain.entity;

// CHECKSTYLE:OFF

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

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

@Entity
@Table(name = "connect_cluster")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ConnectClusterDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "connect REST API 地址", required = true)
    @Column(name = "connect_rest_api_url", length = 3072, nullable = false)
    private String connectRestApiUrl;

    @ApiModelProperty("集群描述")
    @Column(name = "description", length = 1024)
    private String description;

    @ApiModelProperty(value = "版本", required = true)
    @Column(name = "version", length = 32, nullable = false)
    private String version;

    @ApiModelProperty("connector class")
    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"defaultConnectorConfigurations", "connectorType", "dimServiceType"}, allowSetters = true)
    private ConnectorClassDO connectorClass;

    @OneToMany(mappedBy = "connectCluster")
    @Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
    @JsonIgnoreProperties(
            value = {"connectorConfigurations", "creator", "connectorClass", "connectCluster", "kafkaCluster", "desiredState", "actualState"},
            allowSetters = true
    )
    private Set<ConnectorDO> connectors = new HashSet<>();

    public ConnectClusterDO(final Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectClusterDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectClusterDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "ConnectCluster{"
                + "id=" + getId()
                + ", connectRestApiUrl='" + getConnectRestApiUrl() + "'"
                + ", description='" + getDescription() + "'"
                + ", version='" + getVersion() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
