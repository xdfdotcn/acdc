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
@Table(name = "jdbc_sink_connector")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class JdbcSinkConnectorDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "logical_deletion_column", length = 128)
    private String logicalDeletionColumn;

    @Column(name = "logical_deletion_column_value_deletion", length = 32)
    private String logicalDeletionColumnValueDeletion;

    @Column(name = "logical_deletion_column_value_normal", length = 32)
    private String logicalDeletionColumnValueNormal;

    @OneToOne
    @JoinColumn(unique = true)
    private SinkConnectorDO sinkConnector;

    @OneToOne
    @JoinColumn(unique = true)
    private RdbInstanceDO rdbInstance;

    @ManyToOne
    @JsonIgnoreProperties(value = {"rdbDatabase"}, allowSetters = true)
    private RdbTableDO rdbTable;
}
