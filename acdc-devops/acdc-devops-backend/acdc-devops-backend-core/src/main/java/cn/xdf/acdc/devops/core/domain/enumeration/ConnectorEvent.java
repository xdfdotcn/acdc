package cn.xdf.acdc.devops.core.domain.enumeration;

import lombok.Getter;

@Getter
public enum ConnectorEvent {
    STARTUP(EventType.USER_TRIGGER),
    TIMEOUT(EventType.RUNTIME),
    STARTUP_SUCCESS(EventType.RUNTIME),
    UPDATE(EventType.RUNTIME),
    UPDATE_SUCCESS(EventType.RUNTIME),
    STOP(EventType.USER_TRIGGER),
    STOP_SUCCESS(EventType.RUNTIME),
    RESTART(EventType.USER_TRIGGER),
    CREATE_FAILURE(EventType.RUNTIME),
    TASK_FAILURE(EventType.RUNTIME),
    RETRY(EventType.RUNTIME);

    private final EventType type;

    ConnectorEvent(final EventType type) {
        this.type = type;
    }
}
