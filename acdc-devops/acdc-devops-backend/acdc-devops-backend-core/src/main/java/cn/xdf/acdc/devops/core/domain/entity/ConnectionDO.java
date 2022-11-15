package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "connection")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ConnectionDO extends SoftDeletableDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source_data_system_type", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private DataSystemType sourceDataSystemType;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private ProjectDO sourceProject;

    @Column(name = "source_data_set_id", nullable = false)
    private Long sourceDataSetId;

    @OneToOne(fetch = FetchType.LAZY)
    private ConnectorDO sourceConnector;

    @Column(name = "sink_data_system_type", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private DataSystemType sinkDataSystemType;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private ProjectDO sinkProject;

    @Column(name = "sink_instance_id")
    private Long sinkInstanceId;

    @Column(name = "sink_data_set_id", nullable = false)
    private Long sinkDataSetId;

    @OneToOne(fetch = FetchType.LAZY)
    private ConnectorDO sinkConnector;

    @Column(name = "specific_configuration", length = 1024)
    private String specificConfiguration;

    @Column(name = "version", nullable = false)
    private Integer version;

    @Column(name = "requisition_state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private RequisitionState requisitionState;

    @Column(name = "desired_state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private ConnectionState desiredState;

    @Column(name = "actual_state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private ConnectionState actualState;

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO user;

    public ConnectionDO(final Long id) {
        this.id = id;
    }
}
