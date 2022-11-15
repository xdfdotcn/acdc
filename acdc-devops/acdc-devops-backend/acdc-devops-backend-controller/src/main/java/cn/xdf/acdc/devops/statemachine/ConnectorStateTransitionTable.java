package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventLevel;
import cn.xdf.acdc.devops.statemachine.actions.CreatingConnectorAction;
import cn.xdf.acdc.devops.statemachine.actions.RestartingConnectorAction;
import cn.xdf.acdc.devops.statemachine.actions.StoppingConnectorAction;
import cn.xdf.acdc.devops.statemachine.actions.UpdateStateToDbAction;
import cn.xdf.acdc.devops.statemachine.actions.UpdatingConnectorAction;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public enum ConnectorStateTransitionTable {

    PENDING_ON_STARTUP_TO_STARTING(
        ConnectorState.PENDING, ConnectorState.STARTING, ConnectorEvent.STARTUP, CreatingConnectorAction.class, EventLevel.INFO, "connector is starting..."),

    PENDING_ON_CREATE_FAILURE_TO_CREATION_FAILED(
        ConnectorState.PENDING, ConnectorState.CREATION_FAILED, ConnectorEvent.CREATE_FAILURE, UpdateStateToDbAction.class, EventLevel.ERROR, "start failed, error info: \n"),

    CREATION_FAILED_ON_RETRY_TO_PENDING(
        ConnectorState.CREATION_FAILED, ConnectorState.PENDING, ConnectorEvent.RETRY, UpdateStateToDbAction.class, EventLevel.INFO, "connector is restarting..."),

    CREATION_FAILED_ON_STOP_TO_STOPPED(
        ConnectorState.CREATION_FAILED, ConnectorState.STOPPED, ConnectorEvent.STOP, UpdateStateToDbAction.class, EventLevel.INFO, "connector is stopped."),

    PENDING_ON_STOP_TO_STOPPING(
        ConnectorState.PENDING, ConnectorState.STOPPING, ConnectorEvent.STOP, StoppingConnectorAction.class, EventLevel.INFO, "connector is stopping..."),

    STARTING_ON_TIMEOUT_TO_PENDING(
        ConnectorState.STARTING, ConnectorState.PENDING, ConnectorEvent.TIMEOUT, UpdateStateToDbAction.class, EventLevel.INFO, "connector start timeout, wait to retry..."),

    STARTING_ON_STARTUP_SUCCESS_TO_RUNNING(
        ConnectorState.STARTING, ConnectorState.RUNNING, ConnectorEvent.STARTUP_SUCCESS, UpdateStateToDbAction.class, EventLevel.INFO, "connector is running."),

    STARTING_ON_TASK_FAILURE_TO_RUNTIME_FAILED(
        ConnectorState.STARTING, ConnectorState.RUNTIME_FAILED, ConnectorEvent.TASK_FAILURE, UpdateStateToDbAction.class, EventLevel.ERROR, "connector run with failure, error info: \n"),

    RUNTIME_FAILED_ON_RETRY_TO_STARTING(
        ConnectorState.RUNTIME_FAILED, ConnectorState.STARTING, ConnectorEvent.RETRY, RestartingConnectorAction.class, EventLevel.INFO, "connector run failed, restarting..."),

    RUNTIME_FAILED_ON_STOP_TO_STOPPING(
        ConnectorState.RUNTIME_FAILED, ConnectorState.STOPPING, ConnectorEvent.STOP, StoppingConnectorAction.class, EventLevel.INFO, "connector is stopping..."),

    RUNNING_ON_TASK_FAILURE_TO_RUNTIME_FAILED(
        ConnectorState.RUNNING, ConnectorState.RUNTIME_FAILED, ConnectorEvent.TASK_FAILURE, UpdateStateToDbAction.class, EventLevel.ERROR, "connector run with failure, error info: \n"),

    RUNNING_ON_UPDATE_TO_UPDATING(
        ConnectorState.RUNNING, ConnectorState.UPDATING, ConnectorEvent.UPDATE, UpdatingConnectorAction.class, EventLevel.INFO, "connector config has been updated, updating...."),

    UPDATING_ON_UPDATE_SUCCESS_TO_RUNNING(
        ConnectorState.UPDATING, ConnectorState.RUNNING, ConnectorEvent.UPDATE_SUCCESS, UpdateStateToDbAction.class, EventLevel.INFO, "connector is updated and running."),

    RUNNING_ON_STOP_TO_STOPPING(
        ConnectorState.RUNNING, ConnectorState.STOPPING, ConnectorEvent.STOP, StoppingConnectorAction.class, EventLevel.INFO, "connector is stopping..."),

    STOPPING_ON_TIMEOUT_TO_RUNNING(
        ConnectorState.STOPPING, ConnectorState.RUNNING, ConnectorEvent.TIMEOUT, UpdateStateToDbAction.class, EventLevel.INFO, "connector stop timeout, wait to retry..."),

    STOPPING_ON_STOP_SUCCESS_TO_STOPPED(
        ConnectorState.STOPPING, ConnectorState.STOPPED, ConnectorEvent.STOP_SUCCESS, UpdateStateToDbAction.class, EventLevel.INFO, "connector is stopped."),

    STOPPED_ON_RESTART_TO_PENDING(
        ConnectorState.STOPPED, ConnectorState.PENDING, ConnectorEvent.RESTART, UpdateStateToDbAction.class, EventLevel.INFO, "connector is restarting...");

    private static Map<Pair<ConnectorState, ConnectorState>, ConnectorStateTransitionTable> infos = new HashMap<>();

    static {
        infos = Arrays.stream(ConnectorStateTransitionTable.values())
            .collect(Collectors.toMap(table -> Pair.of(table.getFrom(), table.getTo()), table -> table));
    }

    private final ConnectorState from;

    private final ConnectorState to;

    private final ConnectorEvent event;

    private final Class actionClass;

    private final EventLevel eventLevel;

    private final String eventMessage;

    /**
     * Construct a instance with all args.
     * @param from current connector state
     * @param to next connector state
     * @param event connector event
     * @param actionClass action class
     * @param eventLevel event level
     * @param eventMessage event message
     */
    ConnectorStateTransitionTable(final ConnectorState from, final ConnectorState to, final ConnectorEvent event,
        final Class actionClass, final EventLevel eventLevel, final String eventMessage) {

        this.from = from;
        this.to = to;
        this.event = event;
        this.actionClass = actionClass;
        this.eventLevel = eventLevel;
        this.eventMessage = eventMessage;
    }

    /**
     * Get event info by state.
     *
     * @param from ConnectorState
     * @param to ConnectorState
     * @return state machine event info
     */
    public static ConnectorStateTransitionTable getEventInfoByState(final ConnectorState from, final ConnectorState to) {
        return infos.get(Pair.of(from, to));
    }

}
