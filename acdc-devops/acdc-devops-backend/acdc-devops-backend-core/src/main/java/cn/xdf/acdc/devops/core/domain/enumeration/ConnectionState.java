package cn.xdf.acdc.devops.core.domain.enumeration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// CHECKSTYLE:OFF
public enum ConnectionState {

    STARTING,

    RUNNING,

    STOPPING,

    STOPPED,

    FAILED;

    private static final Map<Integer, ConnectionState> CODE_MAP = new HashMap<>();

    static {
        CODE_MAP.put(0, STARTING);
        CODE_MAP.put(1, RUNNING);
        CODE_MAP.put(2, STOPPING);
        CODE_MAP.put(3, STOPPED);
        CODE_MAP.put(4, FAILED);
    }

    public static ConnectionState codeOf(int code) {
        ConnectionState matchType = CODE_MAP.get(code);
        return Optional.of(matchType).get();
    }
}
