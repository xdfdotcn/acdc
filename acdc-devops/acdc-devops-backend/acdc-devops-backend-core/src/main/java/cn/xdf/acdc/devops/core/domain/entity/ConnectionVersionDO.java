package cn.xdf.acdc.devops.core.domain.entity;

import com.google.common.base.Preconditions;

// TODO：move to another package
// CHECKSTYLE:OFF
public final class ConnectionVersionDO {

    public static final Integer INIT_VERSION = 0;

    private Integer currentVersion;

    private ConnectionVersionDO(Integer currentVersion) {
        Preconditions.checkArgument(currentVersion >= 0);
        this.currentVersion = currentVersion;
    }

    public static ConnectionVersionDO of(Integer version) {
        return new ConnectionVersionDO(version <= 0 ? INIT_VERSION : version);
    }

    public static ConnectionVersionDO initVersion() {
        return ConnectionVersionDO.of(INIT_VERSION);
    }

    /**
     * Increment Version.
     *
     * @return next version
     */
    public Integer incrementVersion() {
        return ++currentVersion;
    }

    /**
     * Current Version.
     *
     * @return version
     */
    public Integer version() {
        return this.currentVersion;
    }
}
