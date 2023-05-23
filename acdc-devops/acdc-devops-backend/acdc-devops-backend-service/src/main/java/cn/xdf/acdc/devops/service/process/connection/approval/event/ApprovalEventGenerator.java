package cn.xdf.acdc.devops.service.process.connection.approval.event;

public interface ApprovalEventGenerator {
    
    String DEFAULT_APPROVAL_POLICY = "DEFAULT";
    
    String SKIP_ALL_APPROVAL_POLICY = "SKIP_ALL";
    
    /**
     * 根据操作类型,生成状态机需要的事件.
     *
     * @param id id
     * @param approvalOperation approvalOperation
     * @return ApprovalEvent
     */
    ApprovalEvent generateApprovalEvent(Long id, ApprovalOperation approvalOperation);
}
