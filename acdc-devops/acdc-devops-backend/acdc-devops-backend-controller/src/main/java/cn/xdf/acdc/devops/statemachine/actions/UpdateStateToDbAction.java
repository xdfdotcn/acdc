package cn.xdf.acdc.devops.statemachine.actions;

import cn.xdf.acdc.devops.dto.Connector;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.service.aop.Event;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import cn.xdf.acdc.devops.statemachine.ConnectorStateMachine;
import org.springframework.stereotype.Component;
import org.squirrelframework.foundation.fsm.Action;

@Component
public class UpdateStateToDbAction implements Action<ConnectorStateMachine, ConnectorState, ConnectorEvent, Connector> {

    public static final String EVENT_LEVEL_EXPRESSION =
            "T(cn.xdf.acdc.devops.statemachine.ConnectorStateTransitionTable).getEventInfoByState(#from, #to).getEventLevel()";

    public static final String EVENT_MESSAGE_EXPRESSION =
            "T(cn.xdf.acdc.devops.statemachine.ConnectorStateTransitionTable).getEventInfoByState(#from, #to).getEventMessage().concat(#connector.remark)";

    private ConnectorService connectorService;

    public UpdateStateToDbAction(final ConnectorService connectorService) {
        this.connectorService = connectorService;
    }

    @Override
    @Event(connectorId = "#connector.id", reason = EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED, source = EventSource.ACDC_SCHEDULER,
            level = EVENT_LEVEL_EXPRESSION, message = EVENT_MESSAGE_EXPRESSION)
    public void execute(final ConnectorState from, final ConnectorState to, final ConnectorEvent event, final Connector connector, final ConnectorStateMachine stateMachine) {
        connectorService.updateActualState(connector.getId(), to);
    }

    @Override
    public String name() {
        return "UpdateStateToDbAction";
    }

    @Override
    public int weight() {
        return 0;
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public long timeout() {
        return 0;
    }
}
