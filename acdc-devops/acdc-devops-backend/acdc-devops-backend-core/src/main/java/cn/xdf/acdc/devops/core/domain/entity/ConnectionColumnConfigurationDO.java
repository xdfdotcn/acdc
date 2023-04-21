package cn.xdf.acdc.devops.core.domain.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "connection_column_configuration")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
//@EqualsAndHashCode
@Accessors(chain = true)
public class ConnectionColumnConfigurationDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private ConnectionDO connection;

    @Column(name = "connection_version", nullable = false)
    private Integer connectionVersion;

    @Column(name = "source_column_name", nullable = false)
    private String sourceColumnName;

    @ApiModelProperty("源字段类型")
    @Column(name = "source_column_type")
    private String sourceColumnType;

    @ApiModelProperty("源字段唯一索引名称")
    @Column(name = "source_column_unique_index_names")
    private String sourceColumnUniqueIndexNames;

    @Column(name = "sink_column_name", nullable = false)
    private String sinkColumnName;

    @ApiModelProperty("目标字段类型")
    @Column(name = "sink_column_type")
    private String sinkColumnType;

    @ApiModelProperty("目标字段唯一索引名称")
    @Column(name = "sink_column_unique_index_names")
    private String sinkColumnUniqueIndexNames;

    @Column(name = "filter_operator")
    private String filterOperator;

    @Column(name = "filter_value")
    private String filterValue;

    // TODO
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionColumnConfigurationDO)) {
            return false;
        }

        return id != null && id.equals(((ConnectionColumnConfigurationDO) o).id);
    }
}
