package cn.xdf.acdc.devops.core.domain.entity.enumeration;

import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 实例角色类型.
 */
public enum RoleType {

    MASTER("master"), SLAVE("slave"), DATA_SOURCE("data_source");

    private static final Map<String, RoleType> NAME_ROLE_MAP = new HashMap<>();

    static {
        for (RoleType type : RoleType.values()) {
            String name = type.name;
            NAME_ROLE_MAP.put(name, type);
        }
    }

    private String name;

    RoleType(final String name) {
        this.name = name;
    }

    /**
     * To RoleType.
     *
     * @param name name
     * @return RoleType
     */
    public static RoleType nameOf(final String name) {
        if (Strings.isNullOrEmpty(name)) {
            return MASTER;
        }

        RoleType matchType = NAME_ROLE_MAP.get(name);
        return Objects.isNull(matchType) ? MASTER : matchType;
    }
}
