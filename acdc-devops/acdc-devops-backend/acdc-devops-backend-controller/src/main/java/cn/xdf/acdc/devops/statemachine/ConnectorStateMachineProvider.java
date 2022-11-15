package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import org.springframework.stereotype.Component;
import org.squirrelframework.foundation.fsm.StateMachineBuilder;

@Component
public class ConnectorStateMachineProvider {

    private final StateMachineBuilder<ConnectorStateMachine, ConnectorState, ConnectorEvent, ConnectorInfoDTO> stateMachineBuilder;

    /**
     * Set state machine builder.
     *
     * @param stateMachineBuilder state machine builder
     */
    public ConnectorStateMachineProvider(final StateMachineBuilder<ConnectorStateMachine, ConnectorState, ConnectorEvent, ConnectorInfoDTO> stateMachineBuilder) {
        this.stateMachineBuilder = stateMachineBuilder;
    }

    /**
     * Get a new connector state machine.
     * @param connectorState init connector state
     * @return connector state machine
     */
    public ConnectorStateMachine getNewOne(final ConnectorState connectorState) {
        return stateMachineBuilder.newStateMachine(connectorState);
    }
}
