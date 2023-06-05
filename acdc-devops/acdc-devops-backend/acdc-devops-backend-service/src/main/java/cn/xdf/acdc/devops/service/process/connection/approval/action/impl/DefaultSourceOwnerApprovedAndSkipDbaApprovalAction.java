package cn.xdf.acdc.devops.service.process.connection.approval.action.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.ApproveEmailSender;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerApprovedAndSkipDbaApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.utility.mail.DomainUser;
import cn.xdf.acdc.devops.service.utility.mail.EmailTemplate;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DefaultSourceOwnerApprovedAndSkipDbaApprovalAction implements SourceOwnerApprovedAndSkipDbaApprovalAction {
    
    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;
    
    @Autowired
    private ApproveEmailSender emailSender;
    
    @Override
    public void action(
            final ApprovalState from,
            final ApprovalState to,
            final ApprovalEvent event,
            final ApprovalContext context,
            final ApprovalStateMachine machine) {
        
        // 1. transform
        Long id = context.getId();
        String domainAccount = context.getOperatorId();
        connectionRequisitionService.checkSourceOwnerPermissions(id, domainAccount);
        String approveResult = context.getDescription();
        
        connectionRequisitionService.updateApproveStateBySourceOwner(id, to, approveResult, domainAccount);
        connectionRequisitionService.approveRequisitionConnections(id);
        
        // 2. send email
        DomainUser proposer = machine.getProposer(id);
        
        List<DomainUser> cc = new ArrayList<>();
        
        emailSender.sendApproveEmail(
                id,
                Lists.newArrayList(proposer),
                cc,
                EmailTemplate.SOURCE_OWNER_APPROVED
        );
    }
}
