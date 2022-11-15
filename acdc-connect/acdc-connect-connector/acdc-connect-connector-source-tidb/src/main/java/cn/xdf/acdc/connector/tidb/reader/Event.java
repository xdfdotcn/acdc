package cn.xdf.acdc.connector.tidb.reader;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Event {

    public static final int DEFAULT_ORDER = 0;

    private final EventType type;

    private final Object data;

    private final int order;

    public Event(final EventType type, final Object data) {
        this.type = type;
        this.data = data;
        this.order = DEFAULT_ORDER;
    }

}
