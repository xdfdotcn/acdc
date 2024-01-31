package cn.xdf.acdc.devops.core.domain.entity.enumeration;

public enum ApprovalBatchState {
    // 0: 未进入审批流程
    PENDING,
    
    // 1: 审批中
    APPROVING,
    
    // 2: 审批通过
    APPROVED,
    
    // 3: 审批拒绝
    REFUSED
}
