package cn.xdf.acdc.devops.core.domain.entity.enumeration;

import lombok.Getter;

@Getter
public enum EventReason {
    
    CONNECTOR_CREATED("ConnectorCreated", 1),
    CONNECTOR_DESIRED_STATUS_CHANGED("ConnectorDesiredStatusChanged", 2),
    CONNECTOR_ACTUAL_STATUS_CHANGED("ConnectorActualStatusChanged", 3),
    EXECUTION_ERROR("ExecutionError", 4);
    
    private final String name;
    
    private final Integer code;
    
    EventReason(final String name, final Integer code) {
        this.name = name;
        this.code = code;
    }
}
