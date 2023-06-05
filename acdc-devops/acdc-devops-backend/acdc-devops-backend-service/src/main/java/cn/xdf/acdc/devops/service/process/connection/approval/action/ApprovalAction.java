package cn.xdf.acdc.devops.service.process.connection.approval.action;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;

public interface ApprovalAction {
    
    /**
     * Execute action.
     *
     * @param from from state
     * @param to to state
     * @param event event
     * @param context context
     * @param machine machine
     */
    void action(ApprovalState from, ApprovalState to, ApprovalEvent event, ApprovalContext context, ApprovalStateMachine machine);
}
