package cn.xdf.acdc.devops.core.domain.dto.enumeration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public enum FieldKeyType {

    PRI(1), UNI(2), MUL(3), NONE(4);

    private static final Map<String, FieldKeyType> TYPE_MAP = new HashMap<>();

    static {
        for (FieldKeyType type : FieldKeyType.values()) {
            String name = type.name();
            TYPE_MAP.put(name, type);
        }
    }

    private final int sort;

    FieldKeyType(int sort) {
        this.sort = sort;
    }

    /**
     * To fieldKeyType.
     *
     * @param name name
     * @return FieldKeyType
     */
    public static FieldKeyType nameOf(final String name) {
        return Objects.isNull(TYPE_MAP.get(name)) ? NONE : TYPE_MAP.get(name);
    }

    /**
     * Get sort.
     *
     * @return sort
     */
    public int getSort() {
        return sort;
    }
}
