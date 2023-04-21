package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
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
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "connection_requisition")
@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectionRequisitionDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source_approve_result")
    private String sourceApproveResult;

    @ManyToOne(fetch = FetchType.LAZY)
    private UserDO sourceApproverUser;

    @Column(name = "dba_approve_result")
    private String dbaApproveResult;

    @ManyToOne(fetch = FetchType.LAZY)
    private UserDO dbaApproverUser;

    @Column(name = "state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private ApprovalState state;

    @Column(name = "description")
    private String description;

    @Column(name = "third_party_id")
    private String thirdPartyId;

    @OneToMany(mappedBy = "connectionRequisition", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private Set<ConnectionRequisitionConnectionMappingDO> connectionRequisitionConnectionMappings = new HashSet<>();

    public ConnectionRequisitionDO(final Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionRequisitionDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectionRequisitionDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
}
