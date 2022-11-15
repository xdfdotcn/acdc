package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "kafka_sink_connector")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class KafkaSinkConnectorDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "key_converter", length = 1024, nullable = false)
    private String keyConverter;

    @Column(name = "value_converter", length = 1024, nullable = false)
    private String valueConverter;

    @OneToOne(optional = false)
    @JoinColumn(unique = true)
    private SinkConnectorDO sinkConnector;

    @ManyToOne(optional = false)
    @JsonIgnoreProperties(value = {"sourceRdbTables", "kafkaCluster"}, allowSetters = true)
    private KafkaTopicDO kafkaTopic;
}
