package cn.xdf.acdc.devops.core.util;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;

import java.util.Objects;
import java.util.Set;

public class RdbUtil {

    /**
     * Does this rdb has data source instance.
     *
     * @param rdbInstances rdbInstances
     * @return boolean
     */
    public static boolean hasDataSourceInstance(final Set<RdbInstanceDO> rdbInstances) {
        if (rdbInstances == null) {
            return false;
        }
        for (RdbInstanceDO rdbInstanceDO : rdbInstances) {
            if (Objects.equals(rdbInstanceDO.getRole(), RoleType.DATA_SOURCE)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Does this rdb has master instance.
     *
     * @param rdbInstances rdbInstances
     * @return true or false
     */
    public static boolean hasMasterInstance(final Set<RdbInstanceDO> rdbInstances) {
        if (rdbInstances == null) {
            return false;
        }
        for (RdbInstanceDO rdbInstanceDO : rdbInstances) {
            if (Objects.equals(rdbInstanceDO.getRole(), RoleType.MASTER)) {
                return true;
            }
        }
        return false;
    }

}
