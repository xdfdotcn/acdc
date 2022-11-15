package cn.xdf.acdc.devops.service.process.connection.approval.event;

import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.error.ApprovalProcessStateMatchErrorException;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Optional;

public abstract class AbstractApprovalEventGenerator implements ApprovalEventGenerator {

    private static final Map<ApprovalOperation, ApprovalEvent> OPERATION_EVENT_MAPPING = Maps.newHashMap();

    @Autowired
    private ApprovalStateMachine approvalStateMachine;

    static {
        OPERATION_EVENT_MAPPING.put(ApprovalOperation.PASS, ApprovalEvent.PASS);
        OPERATION_EVENT_MAPPING.put(ApprovalOperation.REFUSED, ApprovalEvent.REFUSED);
    }

    private ApprovalEvent generateApprovalEventFromMapping(final Long id, final ApprovalOperation approvalOperation) {
        return Optional.ofNullable(OPERATION_EVENT_MAPPING.get(approvalOperation))
                .orElseThrow(() -> new ApprovalProcessStateMatchErrorException(
                        String.format("Error while generate approval event, unknown operation : %s, id: %s", approvalOperation, id)
                ));
    }

    protected ApprovalStateMachine getApprovalStateMachine() {
        return approvalStateMachine;
    }

    @Override
    public ApprovalEvent generateApprovalEvent(final Long id, final ApprovalOperation approvalOperation) {
        ApprovalEvent approvalEvent = doGenerateApprovalEvent(id, approvalOperation);
        approvalStateMachine.verifyEvent(id, approvalEvent);
        return approvalEvent;
    }

    /**
     * 子类执行事件生成的逻辑.
     *
     * @param id                id
     * @param approvalOperation approvalOperation
     * @return ApprovalEvent
     */
    private ApprovalEvent doGenerateApprovalEvent(final Long id, final ApprovalOperation approvalOperation) {
        return generateApprovalEventFromMapping(id, approvalOperation);
    }
}
