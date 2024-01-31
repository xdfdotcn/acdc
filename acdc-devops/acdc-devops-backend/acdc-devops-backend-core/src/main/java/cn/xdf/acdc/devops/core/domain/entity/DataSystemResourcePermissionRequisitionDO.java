package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
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
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * 数据系统资源权限申请.
 */
@Data
@Entity
@ApiModel("数据系统资源权限申请")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@Table(name = "data_system_resource_permission_requisition")
public class DataSystemResourcePermissionRequisitionDO extends BaseDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "third_party_id")
    @ApiModelProperty("三方审批系统中的审批单 id")
    private String thirdPartyId;
    
    @ApiModelProperty("申请理由")
    @Column(name = "description")
    private String description;
    
    @ApiModelProperty("申请人")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO user;
    
    @ApiModelProperty("申请人所属项目")
    @ManyToOne(fetch = FetchType.LAZY)
    private ProjectDO sinkProject;
    
    @ApiModelProperty("数据源所属项目")
    @ManyToOne(fetch = FetchType.LAZY)
    private ProjectDO sourceProject;
    
    @ApiModelProperty("数据源负责人审批人")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO sourceApproverUser;
    
    @ApiModelProperty("数据源负责人审批意见")
    @Column(name = "source_approval_comments")
    private String sourceApprovalComments;
    
    @ApiModelProperty("dba 审批人")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO dbaApproverUser;
    
    @ApiModelProperty("dba 审批意见")
    @Column(name = "dba_approval_comments")
    private String dbaApprovalComments;
    
    @Column(name = "state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    @ApiModelProperty("状态;0: 待审批 1: 待数据源负责人审批 2: 数据源负责人审批拒绝 3: 待DBA负责人审批 4: DBA 负责人审批拒绝 5: 审批通过")
    private ApprovalState state;
    
    @ManyToMany(fetch = FetchType.LAZY, mappedBy = "permissionRequisitions")
    private Set<DataSystemResourcePermissionRequisitionBatchDO> requisitionBatches;
    
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "d_s_r_permission_requisition_d_s_r_mapping",
            joinColumns = @JoinColumn(name = "data_system_resource_permission_requisition_id"),
            inverseJoinColumns = @JoinColumn(name = "data_system_resource_id")
    )
    private Set<DataSystemResourceDO> dataSystemResources;
    
    // CHECKSTYLE:OFF
    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        
        final DataSystemResourcePermissionRequisitionDO that = (DataSystemResourcePermissionRequisitionDO) o;
        
        if (!Objects.equals(id, that.id))
            return false;
        if (!Objects.equals(thirdPartyId, that.thirdPartyId))
            return false;
        if (!Objects.equals(description, that.description))
            return false;
        if (!Objects.equals(user, that.user))
            return false;
        if (!Objects.equals(sinkProject, that.sinkProject))
            return false;
        if (!Objects.equals(sourceProject, that.sourceProject))
            return false;
        if (!Objects.equals(sourceApproverUser, that.sourceApproverUser))
            return false;
        if (!Objects.equals(sourceApprovalComments, that.sourceApprovalComments))
            return false;
        if (!Objects.equals(dbaApproverUser, that.dbaApproverUser))
            return false;
        if (!Objects.equals(dbaApprovalComments, that.dbaApprovalComments))
            return false;
        return state == that.state;
    }
    // CHECKSTYLE:ON
    
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (thirdPartyId != null ? thirdPartyId.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (sinkProject != null ? sinkProject.hashCode() : 0);
        result = 31 * result + (sourceProject != null ? sourceProject.hashCode() : 0);
        result = 31 * result + (sourceApproverUser != null ? sourceApproverUser.hashCode() : 0);
        result = 31 * result + (sourceApprovalComments != null ? sourceApprovalComments.hashCode() : 0);
        result = 31 * result + (dbaApproverUser != null ? dbaApproverUser.hashCode() : 0);
        result = 31 * result + (dbaApprovalComments != null ? dbaApprovalComments.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        return result;
    }
    
    @Override
    public String toString() {
        return "DataSystemResourcePermissionRequisitionDO{"
                + "id=" + id
                + ", thirdPartyId='" + thirdPartyId + '\''
                + ", description='" + description + '\''
                + ", user=" + user
                + ", sinkProject=" + sinkProject
                + ", sourceProject=" + sourceProject
                + ", sourceApproverUser=" + sourceApproverUser
                + ", sourceApprovalComments='" + sourceApprovalComments + '\''
                + ", dbaApproverUser=" + dbaApproverUser
                + ", dbaApprovalComments='" + dbaApprovalComments + '\''
                + ", state=" + state
                + '}';
    }
}
