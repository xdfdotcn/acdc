package cn.xdf.acdc.devops.service.process.connection.approval.definition;

import cn.xdf.acdc.devops.service.process.connection.approval.ActionMapping;

import java.util.Map;
import java.util.Optional;

public class ApprovalStateMachineDefinition {

    private Map<ActionMapping.ActionMappingKey, ActionMapping> stateMachineDefinition;

    public ApprovalStateMachineDefinition(
            final Map<ActionMapping.ActionMappingKey, ActionMapping> stateMachineDefinition
    ) {

        this.stateMachineDefinition = stateMachineDefinition;
    }

    /**
     * 获取链路审批状态机的声明.
     *
     * @return Map
     */
    public Map<ActionMapping.ActionMappingKey, ActionMapping> getStateMachineDefinition() {
        return Optional.of(stateMachineDefinition).get();
    }
}
