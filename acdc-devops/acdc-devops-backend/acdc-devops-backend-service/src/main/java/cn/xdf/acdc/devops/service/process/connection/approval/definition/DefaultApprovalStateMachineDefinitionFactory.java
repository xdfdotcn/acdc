package cn.xdf.acdc.devops.service.process.connection.approval.definition;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.approval.ActionMapping;
import cn.xdf.acdc.devops.service.process.connection.approval.action.DbaApprovedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.DbaRefusedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SendApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SkipAllApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerApprovedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerApprovedAndSkipDbaApprovalAction;
import cn.xdf.acdc.devops.service.process.connection.approval.action.SourceOwnerRefusedAction;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;

import java.util.Map;

public class DefaultApprovalStateMachineDefinitionFactory implements ApprovalStateMachineDefinitionFactory {

    /**
     * 审批流程状态机流程声明, ApproveState: 审批状态. EventType: 审批事件 ActionMapping: 根据审批状态和审批事件获取对应的执行 Action
     */
    private static final Map<ActionMapping.ActionMappingKey, ActionMapping> DEFINITION = ActionMapping.builder()

            // 初始化
            .add(
                    ApprovalState.APPROVING,
                    ApprovalState.SOURCE_OWNER_APPROVING,
                    ApprovalEvent.PASS,
                    SendApprovalAction.class
            )

            // 初始化,跳过所有审批环节
            .add(
                    ApprovalState.APPROVING,
                    ApprovalState.APPROVED,
                    ApprovalEvent.PASS_AND_SKIP_REMAIN,
                    SkipAllApprovalAction.class
            )

            // 源端负责人审批拒绝
            .add(
                    ApprovalState.SOURCE_OWNER_APPROVING,
                    ApprovalState.SOURCE_OWNER_REFUSED,
                    ApprovalEvent.REFUSED,
                    SourceOwnerRefusedAction.class
            )

            // 源端负责人审批通过
            .add(
                    ApprovalState.SOURCE_OWNER_APPROVING,
                    ApprovalState.DBA_APPROVING,
                    ApprovalEvent.PASS,
                    SourceOwnerApprovedAction.class
            )

            // 源端负责人审批拒绝,复审通过
            .add(
                    ApprovalState.SOURCE_OWNER_REFUSED,
                    ApprovalState.DBA_APPROVING,
                    ApprovalEvent.PASS,
                    SourceOwnerApprovedAction.class
            )

            // 源端负责人审批通过,跳过DBA审批
            .add(
                    ApprovalState.SOURCE_OWNER_APPROVING,
                    ApprovalState.APPROVED,
                    ApprovalEvent.PASS_AND_SKIP_REMAIN,
                    SourceOwnerApprovedAndSkipDbaApprovalAction.class
            )

            // 源端负责人审批拒绝,复审通过,跳过DBA审批
            .add(
                    ApprovalState.SOURCE_OWNER_REFUSED,
                    ApprovalState.APPROVED,
                    ApprovalEvent.PASS_AND_SKIP_REMAIN,
                    SourceOwnerApprovedAndSkipDbaApprovalAction.class
            )

            // DBA 审批拒绝
            .add(
                    ApprovalState.DBA_APPROVING,
                    ApprovalState.DBA_REFUSED,
                    ApprovalEvent.REFUSED,
                    DbaRefusedAction.class
            )

            // DBA 审批通过
            .add(
                    ApprovalState.DBA_APPROVING,
                    ApprovalState.APPROVED,
                    ApprovalEvent.PASS,
                    DbaApprovedAction.class
            )

            // DBA 审批拒绝,复审通过
            .add(
                    ApprovalState.DBA_REFUSED,
                    ApprovalState.APPROVED,
                    ApprovalEvent.PASS,
                    DbaApprovedAction.class
            )
            .build();

    @Override
    public ApprovalStateMachineDefinition createApprovalStateMachineDefinition() {
        return new ApprovalStateMachineDefinition(DEFINITION);
    }
}
