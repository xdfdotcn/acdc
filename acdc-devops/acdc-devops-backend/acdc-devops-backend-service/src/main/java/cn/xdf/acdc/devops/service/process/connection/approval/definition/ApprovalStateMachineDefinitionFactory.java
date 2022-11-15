package cn.xdf.acdc.devops.service.process.connection.approval.definition;

public interface ApprovalStateMachineDefinitionFactory {

    /**
     * 创建链路审批状态机声明.
     *
     * @return ApprovalStateMachineDefinition
     */
    ApprovalStateMachineDefinition createApprovalStateMachineDefinition();

}
