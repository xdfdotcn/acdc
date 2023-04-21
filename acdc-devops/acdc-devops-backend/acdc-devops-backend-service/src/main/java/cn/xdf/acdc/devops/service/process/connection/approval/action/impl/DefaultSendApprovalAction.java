package cn.xdf.acdc.devops.service.process.connection.approval.action.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.ApproveEmailSender;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SendApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.utility.mail.DomainUser;
import cn.xdf.acdc.devops.service.utility.mail.EmailTemplate;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DefaultSendApprovalAction implements SendApprovalAction {

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
        connectionRequisitionService.updateApproveState(id, to);

        // 2. send email
        List<DomainUser> sourceOwners = machine.getSourceOwners(id);
        DomainUser proposer = machine.getProposer(id);

        List<DomainUser> cc = new ArrayList<>();
        cc.addAll(Lists.newArrayList(proposer));

        emailSender.sendApproveEmail(
                id,
                sourceOwners,
                cc,
                EmailTemplate.SEND_APPROVAL
        );
    }
}
