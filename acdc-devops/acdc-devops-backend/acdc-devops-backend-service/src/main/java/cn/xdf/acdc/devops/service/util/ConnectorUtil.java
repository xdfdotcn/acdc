package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SourceConstant;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Objects;
import org.springframework.util.CollectionUtils;

public final class ConnectorUtil {

    private ConnectorUtil() {
    }

    /**
     * 获取JDBC URL .
     *
     * @param ip ip
     * @param port port
     * @param database database
     * @param dataSystemType dbType
     * @return JDBC URL
     */
    public static String getJdbcUrl(
        final String ip,
        final int port,
        final String database,
        final DataSystemType dataSystemType
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(ip));
        Preconditions.checkArgument(port > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));
        return new StringBuilder()
            .append(getJdbcSchema(dataSystemType))
            .append(ip)
            .append(CommonConstant.PORT_SEPARATOR)
            .append(port)
            .append(CommonConstant.PATH_SEPARATOR)
            .append(database)
            .toString();
    }

    /**
     * 使用逗号分隔符,生成字符串.
     *
     * @param toJoinList 需要被转换成字符串的集合
     * @return 转换后的字符串
     */
    public static String joinOnComma(final List<String> toJoinList) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(toJoinList));
        return Joiner.on(CommonConstant.COMMA).join(toJoinList);
    }

    /**
     * 使用指定分隔符,生成字符串.
     *
     * @param separator 分隔符
     * @param args 参数数组
     * @return 转换后的字符串
     */
    public static String joinOn(final String separator, final String... args) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(separator));
        Preconditions.checkArgument(Objects.nonNull(args) && args.length > 0);

        return Joiner.on(separator).join(args);
    }

    /**
     * 生成  sink connector 名称.
     *
     * @param dataSystemType dbType
     * @param rdb rdb
     * @param database database
     * @param table table
     * @return connector 名称
     */
    public static String getRdbSinkConnectorName(
        final DataSystemType dataSystemType,
        final String rdb,
        final String database,
        final String table) {
        return Joiner.on(CommonConstant.CABLE).join(new String[]{dataSystemType.getSinkPrefix(), rdb, database, table});
    }

    /**
     * 生成 sink hive connector 名称.
     *
     * @param database database
     * @param table table
     * @return connector 名称
     */
    public static String getHiveSinkConnectorName(
        final String database,
        final String table) {
        return Joiner.on(CommonConstant.CABLE).join(new String[]{DataSystemType.HIVE.getSinkPrefix(), database, table});
    }

    /**
     * 生成 sink kafka connector 名称.
     *
     * @param cluster database
     * @param topic topic
     * @return connector 名称
     */
    public static String getKafkaSinkConnectorName(
        final String cluster,
        final String topic) {
        return Joiner.on(CommonConstant.CABLE).join(new String[]{DataSystemType.KAFKA.getSinkPrefix(), cluster, topic});
    }

    /**
     * 获取数据topic 名称.
     *
     * @param dataSystemType dbType
     * @param rdb rdb
     * @param database database
     * @param table table
     * @return topic 名称
     */
    public static String getDataTopic(
        final DataSystemType dataSystemType,
        final String rdb,
        final String database,
        final String table
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rdb));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(table));

        return new StringBuilder()
            .append(dataSystemType.getSourcePrefix())
            .append(CommonConstant.CABLE)
            .append(rdb)
            .append(CommonConstant.CABLE)
            .append(database)
            .append(CommonConstant.CABLE)
            .append(table)
            .toString();
    }

    /**
     * 获取 tidb source 消费topic 名称.
     *
     * @param rdb 集群
     * @param database 数据库
     * @return topic
     */
    public static String geSourceTidbTopic(
        final String rdb,
        final String database
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rdb));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));

        return new StringBuilder()
            .append(DataSystemType.TIDB.getName())
            .append(CommonConstant.CABLE)
            .append(rdb)
            .append(CommonConstant.CABLE)
            .append(database)
            .toString();
    }

    /**
     * 获取 schema 历史 topic 名称.
     *
     * @param dataSystemType dbType
     * @param rdb rdb
     * @param database database
     * @return topic 名称
     */
    public static String getSchemaHistoryTopic(
        final DataSystemType dataSystemType,
        final String rdb,
        final String database
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rdb));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));

        return new StringBuilder()
            .append(SourceConstant.SCHEMA_CHANGE_TOPIC_PREFIX)
            .append(CommonConstant.CABLE)
            .append(dataSystemType.getSourcePrefix())
            .append(CommonConstant.CABLE)
            .append(rdb)
            .append(CommonConstant.CABLE)
            .append(database)
            .toString();
    }

    /**
     * 获取 server topic 名称.
     *
     * @param dataSystemType dbType
     * @param rdb rdb
     * @param database database
     * @return topic 名称
     */
    public static String getSourceServerTopic(
        final DataSystemType dataSystemType,
        final String rdb,
        final String database
    ) {
        return getSourceServerName(dataSystemType, rdb, database);
    }

    /**
     * 获取 server 名称.
     *
     * @param dataSystemType dbType
     * @param rdb rdb
     * @param database database
     * @return topic 名称
     */
    public static String getSourceServerName(
        final DataSystemType dataSystemType,
        final String rdb,
        final String database
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rdb));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));

        return new StringBuilder()
            .append(dataSystemType.getSourcePrefix())
            .append(CommonConstant.CABLE)
            .append(rdb)
            .append(CommonConstant.CABLE)
            .append(database)
            .toString();
    }

    /**
     * 获取 主键配置.
     *
     * @param database database
     * @param table table
     * @param pks pks
     * @return 主键配置
     */
    public static String getMessageKeyColumns(
        final String database,
        final String table,
        final List<String> pks) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(table));
        Preconditions.checkArgument(!CollectionUtils.isEmpty(pks));

        String databaseTable = new StringBuilder()
            .append(database)
            .append(CommonConstant.DOT)
            .append(table)
            .append(CommonConstant.COLON)
            .toString();
        String pkColumns = Joiner.on(CommonConstant.COMMA).join(pks);

        return new StringBuilder()
            .append(databaseTable)
            .append(pkColumns)
            .toString();
    }

    /**
     * 获取 包含的表的配置.
     *
     * @param database database
     * @param table table
     * @return 配置
     */
    public static String getTableInclude(
        final String database,
        final String table) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(table));

        return new StringBuilder()
            .append(database)
            .append(CommonConstant.DOT)
            .append(table)
            .toString();
    }

    private static String getJdbcSchema(final DataSystemType dataSystemType) {
        switch (dataSystemType) {
            case MYSQL:
            case TIDB:
            case HIVE:
                return CommonConstant.MYSQL_SCHEMA;
            default:
                throw new IllegalArgumentException("UNKNOWN type: " + dataSystemType);
        }
    }
}
