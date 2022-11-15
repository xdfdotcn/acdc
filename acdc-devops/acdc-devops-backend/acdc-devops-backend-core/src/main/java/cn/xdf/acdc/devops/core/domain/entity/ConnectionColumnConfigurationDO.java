package cn.xdf.acdc.devops.core.domain.entity;

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
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "connection_column_configuration")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ConnectionColumnConfigurationDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne(fetch = FetchType.LAZY, optional = false)
    private ConnectionDO connection;

    @Column(name = "connection_version", nullable = false)
    private Integer connectionVersion;

    @Column(name = "source_column_name", nullable = false)
    private String sourceColumnName;

    @Column(name = "sink_column_name", nullable = false)
    private String sinkColumnName;

    @Column(name = "filter_operator")
    private String filterOperator;

    @Column(name = "filter_value")
    private String filterValue;
}
