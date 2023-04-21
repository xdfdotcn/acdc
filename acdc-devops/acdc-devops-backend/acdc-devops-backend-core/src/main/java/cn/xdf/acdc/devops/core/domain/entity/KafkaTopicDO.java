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
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;

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

    @ApiModelProperty("关联的 data system resource")
    @OneToOne(fetch = FetchType.LAZY)
    @JoinTable(
            name = "source_data_system_resource_kafka_topic_mapping",
            joinColumns = @JoinColumn(name = "kafka_topic_id"),
            inverseJoinColumns = @JoinColumn(name = "source_data_system_resource_id")
    )
    private DataSystemResourceDO dataSystemResource;

    public KafkaTopicDO(final Long id) {
        this.id = id;
    }

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
