package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;

/**
 * 数据系统资源申请单集合.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class DataSystemResourcePermissionRequisitionDTO {
    
    private Long id;
    
    private ApprovalState state;
    
    private String thirdPartyId;
    
    private String description;
    
    private Long userId;
    
    private String userDomainAccount;
    
    private Long sourceProjectId;
    
    private String sourceProjectName;
    
    private Long sinkProjectId;
    
    private String sinkProjectName;
    
    private String sourceApproverDomainAccount;
    
    private String dbaApproverDomainAccount;
    
    private String sourceApprovalComments;
    
    private String dbaApprovalComments;
    
    private Date creationTime;
    
    private Date updateTime;
    
    private boolean deleted;
    
    public DataSystemResourcePermissionRequisitionDTO(final DataSystemResourcePermissionRequisitionDO requisitionDO) {
        this.id = requisitionDO.getId();
        this.state = requisitionDO.getState();
        this.updateTime = requisitionDO.getUpdateTime();
        this.thirdPartyId = requisitionDO.getThirdPartyId();
        this.description = requisitionDO.getDescription();
        this.userId = requisitionDO.getUser().getId();
        this.userDomainAccount = requisitionDO.getUser().getDomainAccount();
        this.sourceProjectId = requisitionDO.getSourceProject().getId();
        this.sourceProjectName = requisitionDO.getSourceProject().getName();
        this.sinkProjectId = requisitionDO.getSinkProject().getId();
        this.sinkProjectName = requisitionDO.getSinkProject().getName();
        if (requisitionDO.getSourceApproverUser() != null) {
            this.sourceApproverDomainAccount = requisitionDO.getSourceApproverUser().getDomainAccount();
        }
        if (requisitionDO.getDbaApproverUser() != null) {
            this.dbaApproverDomainAccount = requisitionDO.getDbaApproverUser().getDomainAccount();
        }
        this.sourceApprovalComments = requisitionDO.getSourceApprovalComments();
        this.dbaApprovalComments = requisitionDO.getDbaApprovalComments();
        this.creationTime = requisitionDO.getCreationTime();
        this.deleted = false;
    }
}
