package cn.xdf.acdc.devops.service.process.connection.approval.event;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Set;

@ConditionalOnProperty(
        name = "acdc.approval.policy",
        havingValue = ApprovalEventGenerator.SKIP_ALL_APPROVAL_POLICY
)
@Component
public class SkipAllApprovalEventGenerator extends AbstractApprovalEventGenerator {

    private static final Set<ApprovalState> STATE_SET = Sets.immutableEnumSet(ApprovalState.APPROVING);

    @Autowired
    private ApprovalStateMachine approvalStateMachine;

    @Override
    public ApprovalEvent generateApprovalEvent(final Long id, final ApprovalOperation approvalOperation) {
        ApprovalState currentState = getApprovalStateMachine().currentState(id);
        if (!STATE_SET.contains(currentState)) {
            throw new ApprovalProcessStateMatchErrorException(
                    String.format("Error while generate approval event"
                                    + ", an unsupported status was encountered"
                                    + ", currentState: %s, operation: %s, id: %s",
                            currentState, approvalOperation, id)
            );
        }

        ApprovalEvent event = ApprovalEvent.PASS_AND_SKIP_REMAIN;
        approvalStateMachine.verifyEvent(id, event);
        return event;
    }
}
