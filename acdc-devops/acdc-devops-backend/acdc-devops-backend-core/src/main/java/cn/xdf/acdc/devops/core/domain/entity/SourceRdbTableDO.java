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
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "source_rdb_table")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class SourceRdbTableDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty("需要排除的字段")
    @Column(name = "excluded_columns", length = 1024)
    private String excludedColumns;

    @ApiModelProperty("需要脱敏的字段")
    @Column(name = "masked_columns", length = 1024)
    private String maskedColumns;

    @ApiModelProperty("加密的hash算法")
    @Column(name = "mask_hash_algorithm", length = 128)
    private String maskHashAlgorithm;

    @ApiModelProperty("hash算法的salt")
    @Column(name = "mask_hash_algorithm_salt", length = 1024)
    private String maskHashAlgorithmSalt;

    @ApiModelProperty("connector")
    @ManyToOne
    @JsonIgnoreProperties(
            value = {"connectorConfigurations", "creator", "connectorClass", "connectCluster", "kafkaCluster", "desiredState", "actualState"},
            allowSetters = true
    )
    private ConnectorDO connector;

    @ApiModelProperty("关联的表")
    @ManyToOne
    @JsonIgnoreProperties(value = {"rdbDatabase"}, allowSetters = true)
    private RdbTableDO rdbTable;

    @ApiModelProperty("写入kafka的topic")
    @ManyToOne
    @JsonIgnoreProperties(value = {"kafkaCluster", "sourceRdbTables", "sinkRdbTables"}, allowSetters = true)
    private KafkaTopicDO kafkaTopic;

    @ManyToMany
    @JoinTable(
            name = "rel_source_rdb_table__connector_data_extension",
            joinColumns = @JoinColumn(name = "source_rdb_table_id"),
            inverseJoinColumns = @JoinColumn(name = "connector_data_extension_id")
    )
    @JsonIgnoreProperties(value = {"sourceRdbTables", "sinkRdbTables", "sinkHiveTables"}, allowSetters = true)
    private Set<ConnectorDataExtensionDO> connectorDataExtensions = new HashSet<>();

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public SourceRdbTableDO addConnectorDataExtension(ConnectorDataExtensionDO connectorDataExtension) {
        this.connectorDataExtensions.add(connectorDataExtension);
        connectorDataExtension.getSourceRdbTables().add(this);
        return this;
    }

    public SourceRdbTableDO removeConnectorDataExtension(ConnectorDataExtensionDO connectorDataExtension) {
        this.connectorDataExtensions.remove(connectorDataExtension);
        connectorDataExtension.getSourceRdbTables().remove(this);
        return this;
    }

    // functions for jpa union features
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceRdbTableDO)) {
            return false;
        }
        return id != null && id.equals(((SourceRdbTableDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "SourceRdbTable{"
                + "id=" + getId()
                + ", excludedColumns='" + getExcludedColumns() + "'"
                + ", maskedColumns='" + getMaskedColumns() + "'"
                + ", maskHashAlgorithm='" + getMaskHashAlgorithm() + "'"
                + ", maskHashAlgorithmSalt='" + getMaskHashAlgorithmSalt() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
