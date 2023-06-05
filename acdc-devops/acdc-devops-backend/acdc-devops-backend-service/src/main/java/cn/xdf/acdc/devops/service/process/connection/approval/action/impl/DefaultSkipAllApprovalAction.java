package cn.xdf.acdc.devops.service.process.connection.approval.action.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SkipAllApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultSkipAllApprovalAction implements SkipAllApprovalAction {
    
    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;
    
    @Override
    public void action(
            final ApprovalState from,
            final ApprovalState to,
            final ApprovalEvent event,
            final ApprovalContext context,
            final ApprovalStateMachine machine) {
        
        Long id = context.getId();
        connectionRequisitionService.updateApproveState(id, to);
        connectionRequisitionService.approveRequisitionConnections(id);
    }
}
