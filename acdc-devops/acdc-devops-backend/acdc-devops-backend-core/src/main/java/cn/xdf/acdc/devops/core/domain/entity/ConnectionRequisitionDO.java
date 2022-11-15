package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
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
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "connection_requisition")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
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

    public ConnectionRequisitionDO(final Long id) {
        this.id = id;
    }
}
