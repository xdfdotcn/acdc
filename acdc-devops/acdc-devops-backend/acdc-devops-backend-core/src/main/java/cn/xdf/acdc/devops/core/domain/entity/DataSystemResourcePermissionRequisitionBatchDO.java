package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Set;

/**
 * 数据系统资源申请单集合.
 */
@Data
@Entity
@ApiModel("数据系统资源申请单集合")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@Table(name = "data_system_resource_permission_requisition_batch")
public class DataSystemResourcePermissionRequisitionBatchDO extends BaseDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("申请人")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO user;
    
    @ApiModelProperty("申请理由")
    @Column(name = "description")
    private String description;
    
    @Column(name = "state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    @ApiModelProperty("状态;0: 待审批，1: 审批中，2: 已通过，3: 已拒绝")
    private ApprovalBatchState state;
    
    @OneToOne(mappedBy = "requisitionBatch", fetch = FetchType.LAZY)
    private WideTableDO wideTableDO;
    
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "d_s_r_p_r_b_d_s_r_p_r_mapping",
            joinColumns = @JoinColumn(name = "data_system_resource_permission_requisition_batch_id"),
            inverseJoinColumns = @JoinColumn(name = "data_system_resource_permission_requisition_id")
    )
    private Set<DataSystemResourcePermissionRequisitionDO> permissionRequisitions;
    
    @Override
    public String toString() {
        return "DataSystemResourcePermissionRequisitionBatchDO{"
                + "id=" + id
                + ", user=" + user
                + ", description='" + description + "'"
                + ", state=" + state
                + ", wideTableDO=" + wideTableDO
                + '}';
    }
}
