package cn.xdf.acdc.connector.tidb.reader;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EventType {

    /**
     * Ticdc events.
     */
    ROW_CHANGED_EVENT(1, "ROW_CHANGED_EVENT"),

    DDL_EVENT(2, "DDL_EVENT"),

    RESOLVED_EVENT(3, "RESOLVED_EVENT"),

    /**
     * Custom runner stop event.
     */
    RUNNER_STOP_EVENT(4, "RUNNER_STOP_EVENT");

    private int code;

    private String desc;

}
