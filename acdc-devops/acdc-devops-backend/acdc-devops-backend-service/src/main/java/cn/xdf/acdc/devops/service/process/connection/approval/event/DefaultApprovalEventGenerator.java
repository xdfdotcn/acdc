package cn.xdf.acdc.devops.service.process.connection.approval.event;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(
        name = "acdc.approval.policy",
        havingValue = ApprovalEventGenerator.DEFAULT_APPROVAL_POLICY,
        matchIfMissing = true
)
@Component
public class DefaultApprovalEventGenerator extends AbstractApprovalEventGenerator {

}
