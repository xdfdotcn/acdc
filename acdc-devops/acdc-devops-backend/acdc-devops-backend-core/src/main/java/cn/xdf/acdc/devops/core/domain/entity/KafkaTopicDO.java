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
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "kafka topic 信息")
@Entity
@Table(name = "kafka_topic")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class KafkaTopicDO extends SoftDeletableDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "topic名称", required = true)
    @Column(name = "name", length = 1024, nullable = false)
    private String name;

    @ApiModelProperty(value = "所属kafka集群", required = true)
    @ManyToOne(optional = false)
    @JsonIgnoreProperties(value = {"kafkaTopics", "connectors"}, allowSetters = true)
    private KafkaClusterDO kafkaCluster;

    @OneToMany(mappedBy = "kafkaTopic")
    @JsonIgnoreProperties(value = {"connector", "rdbTable", "kafkaTopic", "connectorDataExtensions"}, allowSetters = true)
    private Set<SourceRdbTableDO> sourceRdbTables = new HashSet<>();

    // functions for jpa union feature
    // CHECKSTYLE:OFF

    public void setSourceRdbTables(Set<SourceRdbTableDO> sourceRdbTables) {
        if (this.sourceRdbTables != null) {
            this.sourceRdbTables.forEach(i -> i.setKafkaTopic(null));
        }
        if (sourceRdbTables != null) {
            sourceRdbTables.forEach(i -> i.setKafkaTopic(this));
        }
        this.sourceRdbTables = sourceRdbTables;
    }

    public KafkaTopicDO addSourceRdbTable(SourceRdbTableDO sourceRdbTable) {
        this.sourceRdbTables.add(sourceRdbTable);
        sourceRdbTable.setKafkaTopic(this);
        return this;
    }

    public KafkaTopicDO removeSourceRdbTable(SourceRdbTableDO sourceRdbTable) {
        this.sourceRdbTables.remove(sourceRdbTable);
        sourceRdbTable.setKafkaTopic(null);
        return this;
    }

    // functions for jpa union feature
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaTopicDO)) {
            return false;
        }
        return id != null && id.equals(((KafkaTopicDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "KafkaTopic{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
