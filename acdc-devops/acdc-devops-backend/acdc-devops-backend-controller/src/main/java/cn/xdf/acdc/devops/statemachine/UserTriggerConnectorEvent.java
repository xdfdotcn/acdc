package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;

public enum UserTriggerConnectorEvent {
    
    PENDING_TO_RUNNING(ConnectorState.PENDING, ConnectorState.RUNNING, ConnectorEvent.STARTUP),
    
    PENDING_TO_STOPPED(ConnectorState.PENDING, ConnectorState.STOPPED, ConnectorEvent.STOP),
    
    RUNNING_TO_STOPPED(ConnectorState.RUNNING, ConnectorState.STOPPED, ConnectorEvent.STOP),
    
    STOPPED_TO_RUNNING(ConnectorState.STOPPED, ConnectorState.RUNNING, ConnectorEvent.RESTART),
    
    RUNTIME_FAILED_TO_STOPPED(ConnectorState.RUNTIME_FAILED, ConnectorState.STOPPED, ConnectorEvent.STOP),
    
    CREATION_FAILED_TO_STOPPED(ConnectorState.CREATION_FAILED, ConnectorState.STOPPED, ConnectorEvent.STOP);
    
    private final ConnectorState actual;
    
    private final ConnectorState desired;
    
    private final ConnectorEvent event;
    
    /**
     * Construct a instance with all args.
     *
     * @param actual actual connector state
     * @param desired desired connector state
     * @param event connector event
     */
    UserTriggerConnectorEvent(final ConnectorState actual, final ConnectorState desired, final ConnectorEvent event) {
        this.actual = actual;
        this.desired = desired;
        this.event = event;
    }
    
    /**
     * Get actual connector state.
     *
     * @return actual connector state
     */
    public ConnectorState getActual() {
        return actual;
    }
    
    /**
     * Get desired connector state.
     *
     * @return desired connector state
     */
    public ConnectorState getDesired() {
        return desired;
    }
    
    /**
     * Get event.
     *
     * @return event
     */
    public ConnectorEvent getEvent() {
        return event;
    }
}
