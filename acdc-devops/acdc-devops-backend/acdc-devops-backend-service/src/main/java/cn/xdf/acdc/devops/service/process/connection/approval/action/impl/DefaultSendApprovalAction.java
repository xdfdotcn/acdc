package cn.xdf.acdc.devops.service.process.connection.approval.action.impl;

import cn.xdf.acdc.devops.core.domain.dto.DomainUserDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionProcessService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.ApproveEmailSender;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SendApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.utility.mail.EmailTemplate;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;

@Component
@Transactional
public class DefaultSendApprovalAction implements SendApprovalAction {

    @Autowired
    private ConnectionRequisitionProcessService connectionRequisitionProcessService;

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
        connectionRequisitionProcessService.updateApproveState(id, to);

        // 2. send email
        List<DomainUserDTO> sourceOwners = machine.getSourceOwner(id);
        DomainUserDTO proposer = machine.getProposer(id);

        List<DomainUserDTO> cc = new ArrayList<>();
        cc.addAll(Lists.newArrayList(proposer));

        emailSender.sendApproveEmail(
                id,
                sourceOwners,
                cc,
                EmailTemplate.SEND_APPROVAL
        );
    }
}
