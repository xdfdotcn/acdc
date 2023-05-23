package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectionRequisitionDTO {
    
    private Long id;
    
    private ApprovalState state;
    
    private String description;
    
    private String sourceApproveResult;
    
    private String sourceApproverEmail;
    
    private String dbaApproveResult;
    
    private String dbaApproverEmail;
    
    private String thirdPartyId;
    
    private Date updateTime;
    
    private Date creationTime;
    
    public ConnectionRequisitionDTO(final ConnectionRequisitionDO requisition) {
        this.id = requisition.getId();
        this.state = requisition.getState();
        this.description = requisition.getDescription();
        this.updateTime = requisition.getUpdateTime();
        this.creationTime = requisition.getCreationTime();
        this.sourceApproveResult = requisition.getSourceApproveResult();
        this.dbaApproveResult = requisition.getDbaApproveResult();
        this.thirdPartyId = requisition.getThirdPartyId();
        
        this.sourceApproverEmail = Objects.isNull(requisition.getSourceApproverUser())
                ? SystemConstant.EMPTY_STRING
                : requisition.getSourceApproverUser().getEmail();
        
        this.dbaApproverEmail = Objects.isNull(requisition.getDbaApproverUser())
                ? SystemConstant.EMPTY_STRING
                : requisition.getDbaApproverUser().getEmail();
    }
    
    public ConnectionRequisitionDTO(
            final ConnectionRequisitionDO requisition,
            final String sourceApproverEmail,
            final String dbaApproverEmail
    ) {
        this.id = requisition.getId();
        this.state = requisition.getState();
        this.description = requisition.getDescription();
        this.updateTime = requisition.getUpdateTime();
        this.creationTime = requisition.getCreationTime();
        this.sourceApproveResult = requisition.getSourceApproveResult();
        this.dbaApproveResult = requisition.getDbaApproveResult();
        this.sourceApproverEmail = sourceApproverEmail;
        this.dbaApproverEmail = dbaApproverEmail;
    }
}
