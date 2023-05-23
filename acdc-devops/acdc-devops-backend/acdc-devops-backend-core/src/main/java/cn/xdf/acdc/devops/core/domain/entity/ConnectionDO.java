package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "connection")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
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
    
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private DataSystemResourceDO sourceDataCollection;
    
    @ManyToOne(fetch = FetchType.LAZY)
    private ConnectorDO sourceConnector;
    
    @Column(name = "sink_data_system_type", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private DataSystemType sinkDataSystemType;
    
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private ProjectDO sinkProject;
    
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private DataSystemResourceDO sinkDataCollection;
    
    @ManyToOne(fetch = FetchType.LAZY)
    private DataSystemResourceDO sinkInstance;
    
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
    
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO user;
    
    @OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumns({
            @JoinColumn(updatable = false, name = "connection_id", referencedColumnName = "id"),
            @JoinColumn(updatable = false, name = "connection_version", referencedColumnName = "version")
    })
    private Set<ConnectionColumnConfigurationDO> connectionColumnConfigurations = new HashSet<>();
    
    @OneToMany(mappedBy = "connection", fetch = FetchType.LAZY)
    private Set<ConnectionRequisitionConnectionMappingDO> connectionRequisitionConnectionMappings = new HashSet<>();
    
    public ConnectionDO(final Long id) {
        this.id = id;
    }
    
    // TODO
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionDO)) {
            return false;
        }
        
        ConnectionDO other = (ConnectionDO) o;
        return id != null && id.equals(other.id);
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
}
