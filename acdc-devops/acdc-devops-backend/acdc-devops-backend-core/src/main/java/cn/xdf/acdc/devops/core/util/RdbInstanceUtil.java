package cn.xdf.acdc.devops.core.util;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;

import java.util.Objects;

public class RdbInstanceUtil {

    /**
     * Get rdb instance unique key.
     *
     * @param rdbInstance rdb instance
     * @return unique key
     */
    public static String rdbInstanceUniqueKeyOf(final RdbInstanceDO rdbInstance) {
        Long rdbId = Objects.isNull(rdbInstance.getRdb()) ? 0L : rdbInstance.getRdb().getId();

        return new StringBuilder()
                .append(rdbInstance.getHost())
                .append(rdbInstance.getPort())
                .append(rdbId)
                .toString();
    }
}
