package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

@Entity
@Table(name = "kafka_cluster")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class KafkaClusterDO extends BaseDO implements Serializable {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty(value = "集群名称", required = true)
    @Column(name = "name", length = 32, nullable = false)
    private String name;
    
    @ApiModelProperty(value = "kafka版本", required = true)
    @Column(name = "version", length = 32, nullable = false)
    private String version;
    
    @ApiModelProperty(value = "服务实例地址", required = true)
    @Column(name = "bootstrap_servers", length = 3072, nullable = false)
    private String bootstrapServers;
    
    @ApiModelProperty("安全认证配置")
    @Column(name = "security_configuration", length = 1024)
    private String securityConfiguration;
    
    @ApiModelProperty("集群类型, 1: 默认集群, 2: ticdc集群(DBA使用), 3: 用户集群")
    @Column(name = "cluster_type", columnDefinition = "TINYINT")
    @Enumerated(EnumType.ORDINAL)
    private KafkaClusterType clusterType;
    
    @ApiModelProperty("集群描述")
    @Column(name = "description", length = 1024)
    private String description;
    
    @OneToMany(mappedBy = "kafkaCluster")
    @JsonIgnoreProperties(value = {"kafkaCluster", "sourceRdbTables", "sinkRdbTables"}, allowSetters = true)
    private Set<KafkaTopicDO> kafkaTopics = new HashSet<>();
    
    @OneToMany(mappedBy = "kafkaCluster")
    @JsonIgnoreProperties(
            value = {"connectorConfigurations", "creator", "connectorClass", "connectCluster", "kafkaCluster", "desiredState", "actualState"},
            allowSetters = true
    )
    private Set<ConnectorDO> connectors = new HashSet<>();
    
    public KafkaClusterDO(final Long id) {
        this.id = id;
    }
    // functions for jpa union features
    // CHECKSTYLE:OFF
    
    public void setKafkaTopics(Set<KafkaTopicDO> kafkaTopics) {
        if (this.kafkaTopics != null) {
            this.kafkaTopics.forEach(i -> i.setKafkaCluster(null));
        }
        if (kafkaTopics != null) {
            kafkaTopics.forEach(i -> i.setKafkaCluster(this));
        }
        this.kafkaTopics = kafkaTopics;
    }
    
    public void setConnectors(Set<ConnectorDO> connectors) {
        if (this.connectors != null) {
            this.connectors.forEach(i -> i.setKafkaCluster(null));
        }
        if (connectors != null) {
            connectors.forEach(i -> i.setKafkaCluster(this));
        }
        this.connectors = connectors;
    }
    
    public KafkaClusterDO addConnector(ConnectorDO connector) {
        this.connectors.add(connector);
        connector.setKafkaCluster(this);
        return this;
    }
    
    public KafkaClusterDO removeConnector(ConnectorDO connector) {
        this.connectors.remove(connector);
        connector.setKafkaCluster(null);
        return this;
    }
    
    // functions for jpa union features
    // CHECKSTYLE:ON
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaClusterDO)) {
            return false;
        }
        return id != null && id.equals(((KafkaClusterDO) o).id);
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
    
    // prettier-ignore
    @Override
    public String toString() {
        return "KafkaCluster{"
                + "id=" + getId()
                + ", version='" + getVersion() + "'"
                + ", bootstrapServers='" + getBootstrapServers() + "'"
                + ", securityConfiguration='" + getSecurityConfiguration() + "'"
                + ", clusterType='" + getClusterType() + "'"
                + ", description='" + getDescription() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
