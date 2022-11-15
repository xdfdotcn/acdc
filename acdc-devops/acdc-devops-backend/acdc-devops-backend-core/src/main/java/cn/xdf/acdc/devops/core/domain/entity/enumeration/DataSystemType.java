package cn.xdf.acdc.devops.core.domain.entity.enumeration;

import cn.xdf.acdc.devops.core.domain.dto.enumeration.ClusterType;
import com.google.common.collect.Sets;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

// TODO: 净化
@Getter
public enum DataSystemType {

    MYSQL(
            "mysql",
            "source-mysql",
            "sink-mysql",
            "io.debezium.connector.mysql.MySqlConnector",
            "cn.xdf.acdc.connect.jdbc.JdbcSinkConnector",
            ClusterType.RDB,
            0
    ),

    TIDB(
            "tidb",
            "source-tidb",
            "sink-tidb",
            "cn.xdf.acdc.connector.tidb.TidbConnector",
            "cn.xdf.acdc.connect.jdbc.JdbcSinkConnector",
            ClusterType.RDB,
            1
    ),

    HIVE(
            "hive",
            "",
            "sink-hive",
            "",
            "cn.xdf.acdc.connect.hdfs.HdfsSinkConnector",
            ClusterType.HIVE,
            2
    ),

    KAFKA(
            "kafka",
            "source-kafka",
            "sink-kafka",
            "",
            "cn.xdf.acdc.connect.kafka.KafkaSinkConnector",
            ClusterType.KAFKA,
            3
    );


    private static final Set<DataSystemType> RDB_SET = Sets.newHashSet(
            MYSQL,
            TIDB
    );

    private static final Map<String, DataSystemType> NAME_MAP = new HashMap<>();

    private static final Map<Integer, DataSystemType> CODE_MAP = new HashMap<>();

    static {
        for (DataSystemType type : DataSystemType.values()) {
            String name = type.getName();
            NAME_MAP.put(name, type);
            CODE_MAP.put(type.code, type);
        }
    }

    private String name;

    private String sourcePrefix;

    private String sinkPrefix;

    private String sourceConnectorClass;

    private String sinkConnectorClass;

    private ClusterType clusterType;

    private Integer code;

    DataSystemType(
            final String name,
            final String sourcePrefix,
            final String sinkPrefix,
            final String sourceConnectorClass,
            final String sinkConnectorClass,
            final ClusterType clusterType,
            final Integer code
    ) {
        this.name = name;
        this.sourcePrefix = sourcePrefix;
        this.sinkPrefix = sinkPrefix;
        this.sourceConnectorClass = sourceConnectorClass;
        this.sinkConnectorClass = sinkConnectorClass;
        this.clusterType = clusterType;
        this.code = code;
    }

    /**
     * 根据名称查找枚举.
     *
     * @param name name
     * @return DbType
     */
    public static DataSystemType nameOf(final String name) {
        return Optional.of(NAME_MAP.get(name)).get();
    }

    /**
     * 是否为Rdb 类型数据库.
     *
     * @param dataSystemType dbType
     * @return boolean
     */
    public static boolean isRdb(final DataSystemType dataSystemType) {
        return RDB_SET.contains(dataSystemType);
    }

    /**
     * 是否为 Tidb 类型数据库.
     *
     * @param dataSystemType dbType
     * @return boolean
     */
    public static boolean isTidb(final DataSystemType dataSystemType) {
        return Objects.equals(dataSystemType, TIDB);
    }

    /**
     * 是否为 Mysql 类型数据库.
     *
     * @param dataSystemType dbType
     * @return boolean
     */
    public static boolean isMysql(final DataSystemType dataSystemType) {
        return Objects.equals(dataSystemType, MYSQL);
    }

    /**
     * To DataSystemType.
     *
     * @param code code
     * @return DataSystemType
     */
    public static DataSystemType codeOf(final Integer code) {
        DataSystemType matchType = CODE_MAP.get(code);
        return Optional.of(matchType).get();
    }
}
