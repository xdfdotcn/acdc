package cn.xdf.acdc.devops.statemachine.actions;

import cn.xdf.acdc.devops.biz.connect.ConnectClusterRest;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventReason;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.dto.Connector;
import cn.xdf.acdc.devops.service.aop.Event;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import cn.xdf.acdc.devops.statemachine.ConnectorStateMachine;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

@Component
public class StoppingConnectorAction extends UpdateStateToDbAction {
    
    private final ConnectClusterRest connectClusterRest;
    
    public StoppingConnectorAction(final ConnectClusterRest connectClusterRest, final ConnectorService connectorService) {
        super(connectorService);
        this.connectClusterRest = connectClusterRest;
    }
    
    @Override
    @Event(connectorId = "#connector.id", reason = EventReason.CONNECTOR_ACTUAL_STATUS_CHANGED, source = EventSource.ACDC_SCHEDULER,
            level = EVENT_LEVEL_EXPRESSION, message = EVENT_MESSAGE_EXPRESSION)
    public void execute(final ConnectorState from, final ConnectorState to, final ConnectorEvent event, final Connector connector, final ConnectorStateMachine stateMachine) {
        try {
            connectClusterRest.deleteConnector(connector.getConnectClusterUrl(), connector.getName());
        } catch (HttpClientErrorException exception) {
            if (!HttpStatus.NOT_FOUND.equals(exception.getStatusCode())) {
                throw exception;
            }
        }
        super.execute(from, to, event, connector, stateMachine);
    }
    
    @Override
    public String name() {
        return "StoppingConnectorAction";
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
