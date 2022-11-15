package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.HostAndPortDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.repository.RdbInstanceRepository;
import cn.xdf.acdc.devops.service.config.RuntimeConfig;
import cn.xdf.acdc.devops.service.error.ErrorMsg.Authorization;
import cn.xdf.acdc.devops.service.error.ErrorMsg.DataSystem;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MysqlHelperService extends AbstractMysqlHelperService {

    //CHECKSTYLE:OFF
    @Autowired
    protected RuntimeConfig runtimeConfig;
    //CHECKSTYLE:ON

    @Autowired
    private RdbInstanceRepository rdbInstanceRepository;

    /**
     * 获取数据库列表.
     *
     * @param ip       ip
     * @param port     port
     * @param username username
     * @param password password
     * @return 返回实例下的所有数据库
     */
    public List<String> showDataBases(final String ip, final int port, final String username, final String password) {
        Preconditions.checkArgument(Strings.isNotBlank(ip), "Ip is empty.");
        Preconditions.checkArgument(port > 0, "Port is illegal.");

        Connection conn = createConnection(urlOfRdbInstance(ip, port), username, password);
        return executeQuery(conn, sqlOfShowDatabase(), rs -> {
            List<String> databases = Lists.newArrayList();
            try {
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }

            return CollectionUtils.isEmpty(databases) ? Collections.EMPTY_LIST : databases;
        });
    }

    /**
     * 获取数据库列表.
     *
     * @param ip        ip
     * @param port      port
     * @param username  username
     * @param password  password
     * @param predicate predicate
     * @return 返回实例下的所有数据库
     */
    public List<String> showDataBases(final String ip, final int port, final String username, final String password, final Predicate<String> predicate) {
        return showDataBases(ip, port, username, password).stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * 获取数据库列表.
     *
     * @param rdb       rdb
     * @param predicate predicate
     * @return 返回实例下的所有数据库
     */
    public List<String> showDataBases(final RdbDO rdb, final Predicate<String> predicate) {
        RdbInstanceDO instance = getRdbDdlReadInstance(rdb);

        String username = rdb.getUsername();
        String password = EncryptUtil.decrypt(rdb.getPassword());
        return showDataBases(instance.getHost(), instance.getPort(), username, password, predicate);
    }

    private RdbInstanceDO getRdbDdlReadInstance(final RdbDO rdb) {
        List<RdbInstanceDO> rdbInstanceDOs = rdbInstanceRepository.findRdbInstanceDOSByRdbId(rdb.getId());

        return rdbInstanceDOs.stream()
                .filter(rdbInstance ->
                        Objects.equals(rdbInstance.getRole(), RoleType.MASTER) || Objects.equals(rdbInstance.getRole(), RoleType.DATA_SOURCE)
                )
                .findFirst()
                .orElseThrow(() -> new ServerErrorException("Master and data source instance is not set."));
    }

    /**
     * Check rdb permissions.
     *
     * @param rdb rdb
     */
    public void checkRdbPermissions(final RdbDO rdb) {
        List<RdbInstanceDO> rdbInstances = new ArrayList<>(rdb.getRdbInstances());
        String username = rdb.getUsername();
        String password = EncryptUtil.decrypt(rdb.getPassword());
        checkUserPermissionsAndBinlogConfiguration(rdbInstances, username, password);
    }

    /**
     * show tables.
     *
     * @param host     host
     * @param port     port
     * @param username username
     * @param password password
     * @param database database
     * @return java.util.List
     */
    public List<String> showTables(final String host, final int port, final String username, final String password, final String database) {
        Preconditions.checkArgument(Strings.isNotBlank(host), "Ip is illegal.");
        Preconditions.checkArgument(port > 0, "Port is illegal.");
        Preconditions.checkArgument(Strings.isNotBlank(database), "Database is illegal.");

        Connection conn = createConnection(urlOfRdbDatabase(host, port, database), username, password);
        return executeQuery(conn, sqlOfShowTable(), rs -> {
            List<String> tables = Lists.newArrayList();
            try {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }

            return CollectionUtils.isEmpty(tables) ? Collections.EMPTY_LIST : tables;
        });
    }

    /**
     * 获取数据库下的所有表.
     *
     * @param rdb      rdb
     * @param database database
     * @return 数据库下的所有表
     */
    public List<String> showTables(final RdbDO rdb, final String database) {
        RdbInstanceDO instance = getRdbDdlReadInstance(rdb);

        String username = rdb.getUsername();
        String password = EncryptUtil.decrypt(rdb.getPassword());
        return showTables(instance.getHost(), instance.getPort(), username, password, database);
    }

    /**
     * 从 rdb 实例中选择一个可用的，读取并返回某张 rdb 表的 ddl 信息.
     *
     * @param hosts    hosts
     * @param username username
     * @param password password
     * @param database database
     * @param table    table
     * @return java.util.List
     */
    public List<FieldDTO> descTable(final Set<HostAndPortDTO> hosts, final String username, final String password, final String database, final String table) {
        HostAndPortDTO host = choiceAvailableInstance(hosts, username, password);
        return descTable(host.getHost(), host.getPort(), username, password, database, table);
    }

    /**
     * 获取表结构.
     *
     * @param ip       ip
     * @param port     port
     * @param username username
     * @param password password
     * @param database database
     * @param table    table
     * @return 表结构
     */
    public List<FieldDTO> descTable(final String ip, final int port, final String username, final String password, final String database, final String table) {
        Preconditions.checkArgument(Strings.isNotBlank(ip), "Ip is illegal.");
        Preconditions.checkArgument(port > 0, "Port is illegal.");
        Preconditions.checkArgument(Strings.isNotBlank(database), "Database is illegal.");
        Preconditions.checkArgument(Strings.isNotBlank(table), "Database is illegal.");

        Connection conn = createConnection(urlOfRdbDatabase(ip, port, database), username, password);
        return executeQuery(conn, sqlOfDescTable(table), rs -> {
            List<FieldDTO> fields = Lists.newArrayList();
            try {
                while (rs.next()) {
                    FieldDTO fieldDTO = FieldDTO.builder()
                            .name(rs.getString(1))
                            .dataType(rs.getString(2))
                            .allowNull(rs.getString(3))
                            .keyType(rs.getString(4))
                            .defaultValue(rs.getString(5))
                            .extra(rs.getString(6))
                            .build();
                    fields.add(fieldDTO);
                }
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }

            if (CollectionUtils.isEmpty(fields)) {
                throw new NotFoundException("Not exist fields,table is: " + table);
            }
            return fields;
        });
    }

    /**
     * 检查用户权限、binlog配置.
     *
     * @param instances 实例列表
     * @param username  rdb用户名
     * @param password  rdb密码
     * @date 2022/8/3 8:35 下午
     */
    public void checkUserPermissionsAndBinlogConfiguration(final List<RdbInstanceDO> instances, final String username, final String password) {
        log.info("checking permissions and binlog configuration for rdb instances '{}'", instances);

        // 只有role为1-master、3-DataSource时需要检查权限，且master和DataSource需要分开校验
        for (RdbInstanceDO instance : instances) {
            if (RoleType.MASTER.equals(instance.getRole())) {
                log.info("checking permissions for user '{}' in master rdb instance '{}:{}'", username, instance.getHost(), instance.getPort());
                checkPermissions(instance.getHost(), instance.getPort(), username, password, Constant.PERMISSIONS_FOR_MASTER);
                checkSqlMode(instance.getHost(), instance.getPort(), username, password);
            } else if (RoleType.DATA_SOURCE.equals(instance.getRole())) {
                log.info("checking permissions for user '{}' in data source rdb instance '{}:{}'", username, instance.getHost(), instance.getPort());
                checkPermissions(instance.getHost(), instance.getPort(), username, password, Constant.PERMISSIONS_FOR_DATASOURCE);
                log.info("checking binlog configuration for data source rdb instance '{}:{}'", instance.getHost(), instance.getPort());
                checkBinlogConfiguration(instance.getHost(), instance.getPort(), username, password);
            }
        }

        log.info("successfully check permissions and binlog configuration for rdb instances '{}'", instances);
    }

    /**
     * 1. 对于host范围: 只要有一个host配置满足一个校验集合即可
     * 2. 对于ip：需要所有ip均校验通过，才返回成功，如果有多个校验集合，满足一个校验集合即可
     */
    protected void checkPermissions(final String host, final Integer port, final String username, final String password, final List<String[]> permissions) {
        // check host range first
        for (String each : runtimeConfig.getHost().getRanges()) {
            try {
                checkPermissions(host, port, username, password, permissions, each);
                if (log.isDebugEnabled()) {
                    log.debug("successfully check permissions for user '{}'@'{}' in rdb instance '{}:{}'", username, each, host, port);
                }
                // 有一个校验通过即可
                return;
            } catch (ServerErrorException e) {
                log.warn(String.format(Authorization.INSUFFICIENT_PERMISSIONS + " for user '%s'@'%s' in rdb instance '%s:%s'", username, each, host, port), e);
            }
        }

        try {
            // if no permissions for host range, check for each ip
            for (String each : runtimeConfig.getHost().getIps()) {
                checkPermissions(host, port, username, password, permissions, each);
            }
        } catch (ServerErrorException e) {
            throw new ServerErrorException(String.format("%s for user '%s'", Authorization.INSUFFICIENT_PERMISSIONS, username), e);
        }
    }

    protected void checkPermissions(final String mysqlHost, final Integer port, final String username, final String password, final List<String[]> requiredPermissions, final String clientHost) {
        String selectGrantsSql = String.format(Constant.SHOW_GRANTS_SQL_PATTERN, username, clientHost);
        Set<String> grantedPermissions = executeQuery(createConnection(urlOfRdbDatabase(mysqlHost, port, ""), username, password), selectGrantsSql, rs -> {
            try {
                Set<String> permissionsInResultSet = Sets.newHashSet();
                while (rs.next()) {
                    // todo: PolarDB-x 's show grants result is different with mysql, which contains three columns.
                    permissionsInResultSet.addAll(extractPermissionsFromShowGrantsResult(rs.getString(1)));
                }
                return permissionsInResultSet;
            } catch (SQLException e) {
                throw new ServerErrorException(String.format("error while execute show grants error for user '%s'@'%s' in rdb instance '%s:%s'", username, clientHost, mysqlHost, port), e);
            }
        });
        if (log.isDebugEnabled()) {
            log.debug("extract permissions successfully. [mysqlHost]:{}, [port]:{}, [permissions]:{}, [readyForCheckHost]:{}", mysqlHost, port, grantedPermissions, clientHost);
        }
        // 判断权限集
        checkPermissions(grantedPermissions, requiredPermissions);
    }

    protected Set<String> extractPermissionsFromShowGrantsResult(final String grantedPermissionsString) {
        /*
         * grantedPermissionsString's format is like:
         * GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%'
         * result's format is like:
         * SELECT,REPLICATION SLAVE,REPLICATION CLIENT
         */
        if (!grantedPermissionsString.contains(Constant.SHOW_GRANTS_RESULT_KEYWORD_GRANT)
                || !grantedPermissionsString.contains(Constant.SHOW_GRANTS_RESULT_KEYWORD_ON)) {
            return Collections.emptySet();
        }
        String substring = grantedPermissionsString.substring(6, grantedPermissionsString.indexOf(Constant.SHOW_GRANTS_RESULT_KEYWORD_ON)).toUpperCase();
        return Arrays.stream(substring.split(Constant.SHOW_GRANTS_RESULT_SPLIT)).map(String::trim).collect(Collectors.toSet());
    }

    //CHECKSTYLE:OFF
    protected void checkPermissions(final Set<String> grantedPermissions, final List<String[]> requiredPermissions) {
        if (grantedPermissions.contains(Constant.ALL_PRIVILEGES)) {
            return;
        }
        for (String[] permissions : requiredPermissions) {
            for (int i = 0; i < permissions.length; i++) {
                if (grantedPermissions.contains(permissions[i])) {
                    break;
                }
                if (i == permissions.length - 1) {
                    log.error("check permission failed for {}", permissions[i]);
                    throw new ServerErrorException(String.format("%s: %s", Authorization.INSUFFICIENT_PERMISSIONS, permissions[i]));
                }
            }
        }
    }
    //CHECKSTYLE:ON

    protected void checkBinlogConfiguration(final String host, final Integer port, final String username, final String password) {
        for (int i = 0; i < Constant.TO_CHECK_BINLOG_CONFIGURATION.length; i++) {
            String result = executeSqlLikeShowVariables(host, port, username, password, Constant.TO_CHECK_BINLOG_CONFIGURATION[i]).toUpperCase();
            // 使用表达式引擎Aviator计算比较结果
            executeExpression(Constant.EXPECTED_BINLOG_CONFIGURATION_VALUE_EXPRESSION[i], Constant.TO_CHECK_BINLOG_CONFIGURATION[i],
                    Constant.EXPECTED_BINLOG_CONFIGURATION_VALUE[i], result);
        }
    }

    protected void checkSqlMode(final String host, final Integer port, final String username, final String password) {
        String result = executeSqlLikeShowVariables(host, port, username, password, Constant.SQL_MODE).toUpperCase();
        if (!result.contains(Constant.EXPECTED_SQL_MODE_VALUE)) {
            log.error("check sql_mode failed. [should have]:{} [actual]:{}", Constant.EXPECTED_SQL_MODE_VALUE, result);
            throw new ServerErrorException(String.format(DataSystem.UNEXPECTED_CONFIGURATION_VALUE, Constant.SQL_MODE, Constant.EXPECTED_SQL_MODE_VALUE, result));
        }
    }

    protected String executeSqlLikeShowVariables(final String host, final Integer port, final String username, final String password, final String variables) {
        String sql = String.format(Constant.SHOW_VARIABLES_SQL, variables);
        return executeQuery(createConnection(urlOfRdbDatabase(host, port, ""), username, password), sql, rs -> {
            try {
                if (rs.next()) {
                    return rs.getString(2);
                }
                return SystemConstant.EMPTY_STRING;
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        });
    }

    protected void executeExpression(final String expression, final String config, final String exceptedValue, final String result) {
        Map<String, Object> param = Maps.newHashMapWithExpectedSize(1);
        if (StringUtils.isNumeric(result)) {
            param.put(Constant.RESULT, Integer.parseInt(result));
        } else {
            param.put(Constant.RESULT, result);
        }
        boolean executeResult = (Boolean) AviatorEvaluator.execute(expression, param);
        if (!executeResult) {
            log.error("check config failed. [config]:{} [expected]:{} [actual]:{}", config, exceptedValue, result);
            throw new ServerErrorException(String.format(DataSystem.UNEXPECTED_CONFIGURATION_VALUE, config, exceptedValue, result));
        }
    }

    private HostAndPortDTO choiceAvailableInstance(final Set<HostAndPortDTO> hosts, final String username, final String password) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(hosts));

        for (HostAndPortDTO it : hosts) {
            try {
                showDataBases(it.getHost(), it.getPort(), username, password);
                return it;
            } catch (ServerErrorException e) {
                log.warn("Execute DDL exception,host: {}, A CDC db user: {}, message: {}", it, username, e.getMessage());
            }
        }
        throw new ServerErrorException("No available rdb instance host: " + hosts);
    }

    protected String sqlOfShowTable() {
        return Constant.SQL_SHOW_TABLES;
    }

    protected String sqlOfShowDatabase() {
        return Constant.SQL_SHOW_DATABASES;
    }

    protected String sqlOfDescTable(final String table) {
        return String.format(Constant.SQL_DESC_TABLE, table);
    }

    protected String urlOfRdbInstance(final String ip, int port) {
        return urlOfRdbDatabase(ip, port, null);
    }

    protected String urlOfRdbDatabase(final String ip, int port, final String database) {
        StringBuilder append = new StringBuilder()
                .append(Constant.JDBC_SCHEMA)
                .append(ip)
                .append(Symbol.PORT_SEPARATOR)
                .append(port);
        if (StringUtils.isNotBlank(database)) {
            append.append(Symbol.URL_PATH_SEPARATOR)
                    .append(database);
        }
        append.append(Constant.CONNECTION_PROPERTY);
        return append.toString();
    }

    /**
     * MysqlHelperService相关常量.
     *
     * @since 2022/9/22 2:04 下午
     */
    static class Constant {

        private static final String SQL_SHOW_DATABASES = " SHOW DATABASES ";

        private static final String SQL_SHOW_TABLES = " SHOW TABLES ";

        private static final String SQL_DESC_TABLE = " DESC `%s` ";

        private static final String RESULT = "result";

        private static final String JDBC_SCHEMA = "jdbc:mysql://";

        private static final String CONNECTION_PROPERTY = "?useSSL=false";

        private static final String SHOW_GRANTS_SQL_PATTERN = "show grants for %s@'%s'";

        private static final String SHOW_GRANTS_RESULT_KEYWORD_ON = " ON ";

        private static final String SHOW_GRANTS_RESULT_KEYWORD_GRANT = "GRANT ";

        private static final String SHOW_GRANTS_RESULT_SPLIT = ",";

        private static final String SHOW_VARIABLES_SQL = "show variables like '%s'";

        private static final String SQL_MODE = "sql_mode";

        private static final String EXPECTED_SQL_MODE_VALUE = "STRICT_TRANS_TABLES";

        private static final String[] TO_CHECK_BINLOG_CONFIGURATION = new String[]{"log_bin", "binlog_format", "binlog_row_image", "expire_logs_days"};

        private static final String[] EXPECTED_BINLOG_CONFIGURATION_VALUE_EXPRESSION = new String[]{"string.contains(result,'ON')",
                "string.contains(result,'ROW')", "string.contains(result,'FULL')", "result>=4 || result==0"};

        private static final String[] EXPECTED_BINLOG_CONFIGURATION_VALUE = new String[]{"ON", "ROW", "FULL", "4"};

        private static final String ALL_PRIVILEGES = "ALL PRIVILEGES";

        private static final List<String[]> PERMISSIONS_FOR_MASTER = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"INSERT"}, new String[]{"UPDATE"},
                new String[]{"DELETE"});

        private static final List<String[]> PERMISSIONS_FOR_DATASOURCE = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"REPLICATION SLAVE"},
                new String[]{"REPLICATION CLIENT"}, new String[]{"RELOAD", "LOCK TABLES"});
    }
}
