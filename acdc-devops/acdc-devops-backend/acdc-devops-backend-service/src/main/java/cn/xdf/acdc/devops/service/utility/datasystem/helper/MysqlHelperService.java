package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.config.RuntimeProperties;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.util.UrlUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A utility service for Mysql, such as show tables of a database.
 *
 * <p>Make sure every password argument is decrypted.
 */
@Slf4j
@Service
public class MysqlHelperService {

    protected static final String SHOW_VARIABLES_SQL = "show variables";

    protected static final String ALL_PRIVILEGES = "ALL PRIVILEGES";

    protected static final String SQL_SHOW_DATABASES = " SHOW DATABASES ";

    protected static final String SQL_SHOW_TABLES = " SHOW TABLES ";

    protected static final String SQL_DESC_TABLE = new StringBuilder()
            .append("SELECT ")
            .append("info_schema_columns.column_name,")
            .append("info_schema_columns.column_type,")
            .append("info_schema_statistics.index_name")
            .append(" FROM ")
            .append("(SELECT  column_name,column_type FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s') info_schema_columns")
            .append(" LEFT JOIN ")
            .append("(SELECT  index_name,column_name FROM information_schema.statistics WHERE non_unique=0 AND table_schema='%s' AND table_name='%s') info_schema_statistics")
            .append(" ON ")
            .append("info_schema_columns.column_name = info_schema_statistics.column_name")
            .toString();

    protected static final String CONNECTION_PROPERTY = "useSSL=false";

    private static final String SHOW_GRANTS_SQL_PATTERN = "show grants for %s@'%s'";

    private static final String SHOW_GRANTS_RESULT_KEYWORD_ON = " ON ";

    private static final String SHOW_GRANTS_RESULT_KEYWORD_GRANT = "GRANT ";

    private static final String SHOW_GRANTS_RESULT_SPLIT = ",";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.warn("Init mysql jdbc driver exception", e);
            throw new ServerErrorException(e);
        }
    }

    private RuntimeProperties runtimeProperties;

    @Autowired
    public MysqlHelperService(final RuntimeProperties runtimeProperties) {
        this.runtimeProperties = runtimeProperties;
    }

    /**
     * Execute a query sql to mysql.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @param sql                 sql
     * @param callback            callback function
     * @param <R>                 callback function method return value
     * @return jdbc query result
     */
    public <R> R executeQuery(
            final HostAndPort hostAndPort,
            final UsernameAndPassword usernameAndPassword,
            final String sql,
            final Function<ResultSet, R> callback) {
        return executeQuery(hostAndPort, usernameAndPassword, null, sql, callback);
    }

    /**
     * Execute a query sql to mysql in a database.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @param database            database name
     * @param sql                 sql
     * @param callback            callback function
     * @param <R>                 callback function method return value
     * @return jdbc query result
     */
    public <R> R executeQuery(
            final HostAndPort hostAndPort,
            final UsernameAndPassword usernameAndPassword,
            final String database,
            final String sql,
            final Function<ResultSet, R> callback) {
        return executeQuery(createConnection(generateMysqlUrl(hostAndPort, database), usernameAndPassword), sql, callback);
    }

    /**
     * Execute a query sql to mysql in a database.
     *
     * @param url                 jdbc url
     * @param usernameAndPassword username and password model
     * @param sql                 sql
     * @param callback            callback function
     * @param <R>                 callback function method return value
     * @return jdbc query result
     */
    public <R> R executeQuery(
            final String url,
            final UsernameAndPassword usernameAndPassword,
            final String sql,
            final Function<ResultSet, R> callback) {
        return executeQuery(createConnection(url, usernameAndPassword), sql, callback);
    }

    /**
     * Execute a query sql to mysql in a database.
     *
     * @param url                 jdbc url
     * @param usernameAndPassword username and password model
     * @param sql                 sql
     * @param prepareFun          preprocessing function
     * @param callback            callback function
     * @param <R>                 callback function method return value
     * @return jdbc query result
     */
    public <R> R executeQuery(
            final String url,
            final UsernameAndPassword usernameAndPassword,
            final String sql,
            final Consumer<PreparedStatement> prepareFun,
            final Function<ResultSet, R> callback) {
        return executeQuery(createConnection(url, usernameAndPassword), sql, prepareFun, callback);
    }

    protected <R> R executeQuery(
            final Connection connection,
            final String sql,
            final Function<ResultSet, R> callback) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = connection;
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            R result = callback.apply(rs);
            return result;
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        } finally {
            close(conn, stmt, rs);
        }
    }

    protected <R> R executeQuery(
            final Connection connection,
            final String sql,
            final Consumer<PreparedStatement> prepareFun,
            final Function<ResultSet, R> callback
    ) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = connection;
            stmt = conn.prepareStatement(sql);
            prepareFun.accept(stmt);
            rs = stmt.executeQuery();
            R result = callback.apply(rs);
            return result;
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        } finally {
            close(conn, stmt, rs);
        }
    }

    /**
     * Show grants of a user and client host.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @param clientHost          client host
     * @return grants set
     */
    public Set<String> showGrants(
            final HostAndPort hostAndPort,
            final UsernameAndPassword usernameAndPassword,
            final String clientHost) {
        String selectGrantsSql = String.format(SHOW_GRANTS_SQL_PATTERN, usernameAndPassword.getUsername(), clientHost);
        return executeQuery(createConnection(generateMysqlUrl(hostAndPort), usernameAndPassword), selectGrantsSql, resultSet -> {
            Set<String> permissions = Sets.newHashSet();
            try {
                while (resultSet.next()) {
                    // todo: PolarDB-x 's show grants result is different with mysql, which contains three columns.
                    permissions.addAll(extractPermissionsFromShowGrantsResult(resultSet.getString(1)));
                }
            } catch (SQLException e) {
                throw new ServerErrorException(
                        String.format("error while execute show grants error for user '%s'@'%s' in rdb instance '%s:%s'", usernameAndPassword.getUsername(), clientHost, hostAndPort.getHost(),
                                hostAndPort.getPort()), e);
            }

            if (log.isDebugEnabled()) {
                log.debug("extract permissions successfully. [mysqlHost]:{}, [port]:{}, [permissions]:{}, [readyForCheckHost]:{}", hostAndPort.getHost(), hostAndPort.getPort(), permissions,
                        clientHost);
            }

            return permissions;
        });
    }

    protected Set<String> extractPermissionsFromShowGrantsResult(final String grantedPermissionsString) {
        /*
         * grantedPermissionsString's format is like:
         * GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%'
         * result's format is like:
         * SELECT,REPLICATION SLAVE,REPLICATION CLIENT
         */
        if (!grantedPermissionsString.contains(SHOW_GRANTS_RESULT_KEYWORD_GRANT)
                || !grantedPermissionsString.contains(SHOW_GRANTS_RESULT_KEYWORD_ON)) {
            return Collections.emptySet();
        }
        String substring = grantedPermissionsString.substring(6, grantedPermissionsString.indexOf(SHOW_GRANTS_RESULT_KEYWORD_ON)).toUpperCase();
        return Arrays.stream(substring.split(SHOW_GRANTS_RESULT_SPLIT)).map(String::trim).collect(Collectors.toSet());
    }

    /**
     * Show variables of a mysql instance.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @return variables map
     */
    public Map<String, String> showVariables(
            final HostAndPort hostAndPort,
            final UsernameAndPassword usernameAndPassword) {
        return executeQuery(createConnection(generateMysqlUrl(hostAndPort), usernameAndPassword), SHOW_VARIABLES_SQL, resultSet -> {
            Map<String, String> variables = new HashMap<>();
            try {
                while (resultSet.next()) {
                    variables.put(resultSet.getString(1), resultSet.getString(2));
                }
            } catch (SQLException e) {
                throw new ServerErrorException(String.format("error while execute show variables at mysql instance '%s:%s'", hostAndPort.getHost(), hostAndPort.getPort()), e);
            }

            if (log.isDebugEnabled()) {
                log.debug("execute show variables successfully, variables: {}", variables);
            }

            return variables;
        });
    }

    /**
     * Get database names of an instance.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @return database name list
     */
    public List<String> showDataBases(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hostAndPort.getHost()), "Ip is empty.");
        Preconditions.checkArgument(hostAndPort.getPort() > 0, "Port is illegal.");

        return executeQuery(hostAndPort, usernameAndPassword, sqlOfShowDatabase(), rs -> {
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
     * Get database names of an instance.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @param predicate           filter function
     * @return database list
     */
    public List<String> showDataBases(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword, final Predicate<String> predicate) {
        return showDataBases(hostAndPort, usernameAndPassword).stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * Get database names of an instances.
     *
     * @param hostAndPorts        host and port model
     * @param usernameAndPassword username and password model
     * @param predicate           filter function
     * @return database list
     */
    public List<String> showDataBases(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword, final Predicate<String> predicate) {
        HostAndPort hostAndPort = choiceAvailableInstance(hostAndPorts, usernameAndPassword);
        return showDataBases(hostAndPort, usernameAndPassword).stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * Get table names of a mysql database.
     *
     * @param hostAndPorts        host and port model
     * @param database            database name
     * @param usernameAndPassword username and password model
     * @return table list
     */
    public List<String> showTables(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword, final String database) {
        HostAndPort hostAndPort = choiceAvailableInstance(hostAndPorts, usernameAndPassword);
        return showTables(hostAndPort, usernameAndPassword, database);
    }

    /**
     * Get table names of a mysql database.
     *
     * @param hostAndPort         host and port model
     * @param database            database name
     * @param usernameAndPassword username and password model
     * @return table list
     */
    public List<String> showTables(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword, final String database) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hostAndPort.getHost()), "Ip is illegal.");
        Preconditions.checkArgument(hostAndPort.getPort() > 0, "Port is illegal.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database), "Database is illegal.");

        Connection conn = createConnection(generateMysqlUrl(hostAndPort, database), usernameAndPassword);
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
     * 从 rdb 实例中选择一个可用的，读取并返回某张 rdb 表的 ddl 信息.
     *
     * @param hostAndPosts        host and port model
     * @param usernameAndPassword username and password model
     * @param database            database name
     * @param table               table name
     * @return table filed list
     */
    public List<RelationalDatabaseTableField> descTable(final Set<HostAndPort> hostAndPosts, final UsernameAndPassword usernameAndPassword, final String database, final String table) {
        HostAndPort hostAndPort = choiceAvailableInstance(hostAndPosts, usernameAndPassword);
        return descTable(hostAndPort, usernameAndPassword, database, table);
    }

    /**
     * 获取表结构.
     *
     * @param hostAndPort         host and port model
     * @param usernameAndPassword username and password model
     * @param database            database name
     * @param table               table name
     * @return table field list
     */
    public List<RelationalDatabaseTableField> descTable(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword, final String database, final String table) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hostAndPort.getHost()), "Ip is illegal.");
        Preconditions.checkArgument(hostAndPort.getPort() > 0, "Port is illegal.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(database), "Database is illegal.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(table), "Database is illegal.");

        Connection conn = createConnection(generateMysqlUrl(hostAndPort, database), usernameAndPassword);
        return executeQuery(conn, sqlOfDescTable(database, table), rs -> {
            Map<String, RelationalDatabaseTableField> nameToFields = new HashMap<>();
            try {
                while (rs.next()) {
                    String columnName = rs.getString(1);
                    String columnType = rs.getString(2);
                    String columnUniqueIndexName = rs.getString(3);

                    nameToFields.computeIfAbsent(columnName, key -> RelationalDatabaseTableField.builder()
                            .name(columnName)
                            .type(columnType)
                            .build());

                    if (!Strings.isNullOrEmpty(columnUniqueIndexName)) {
                        nameToFields.get(columnName).getUniqueIndexNames().add(columnUniqueIndexName);
                    }
                }
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }

            if (nameToFields.isEmpty()) {
                throw new ServerErrorException("Not exist fields, table is: " + table);
            }

            return new ArrayList<>(nameToFields.values());
        });
    }

    /**
     * Check rdb instance permissions.
     *
     * @param hostAndPort         instance host and port
     * @param usernameAndPassword username and password
     * @param requiredPermissions required permissions
     */
    public void checkPermissions(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword,
            final List<String[]> requiredPermissions) {
        // check host range first
        for (String each : runtimeProperties.getHost().getRanges()) {
            try {
                checkPermissions(hostAndPort, usernameAndPassword, requiredPermissions, each);
                if (log.isDebugEnabled()) {
                    log.debug("successfully check permissions for user '{}'@'{}' in rdb instance '{}:{}'", usernameAndPassword.getUsername(), each, hostAndPort.getHost(), hostAndPort.getPort());
                }
                // 有一个校验通过即可
                return;
            } catch (ServerErrorException e) {
                log.warn(String.format(ErrorMsg.Authorization.INSUFFICIENT_PERMISSIONS + " for user '%s'@'%s' in rdb instance '%s:%s'",
                        usernameAndPassword.getUsername(), each, hostAndPort.getHost(), hostAndPort.getPort()), e);
            }
        }

        try {
            // if no permissions for host range, check for each ip
            for (String each : runtimeProperties.getHost().getIps()) {
                checkPermissions(hostAndPort, usernameAndPassword, requiredPermissions, each);
            }
        } catch (ServerErrorException e) {
            throw new ServerErrorException(String.format("%s for user '%s'", ErrorMsg.Authorization.INSUFFICIENT_PERMISSIONS, usernameAndPassword.getUsername()), e);
        }
    }

    protected void checkPermissions(final Set<String> grantedPermissions, final List<String[]> requiredPermissions) {
        if (grantedPermissions.contains(ALL_PRIVILEGES)) {
            return;
        }
        for (String[] permissions : requiredPermissions) {
            for (int i = 0; i < permissions.length; i++) {
                if (grantedPermissions.contains(permissions[i])) {
                    break;
                }
                if (i == permissions.length - 1) {
                    log.error("check permission failed for {}", permissions[i]);
                    throw new ServerErrorException(String.format("%s: %s", ErrorMsg.Authorization.INSUFFICIENT_PERMISSIONS, permissions[i]));
                }
            }
        }
    }

    private void checkPermissions(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword,
            final List<String[]> requiredPermissions, final String clientHost) {
        Set<String> grantedPermissions = this.showGrants(hostAndPort, usernameAndPassword, clientHost);
        // check if permission is enough
        checkPermissions(grantedPermissions, requiredPermissions);
    }

    private HostAndPort choiceAvailableInstance(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword) {
        if (!CollectionUtils.isEmpty(hostAndPorts)) {
            for (HostAndPort each : hostAndPorts) {
                try {
                    showDataBases(each, usernameAndPassword);
                    return each;
                } catch (ServerErrorException e) {
                    log.warn("Execute DDL exception,host: {}, A CDC db user: {}, message: {}", each, usernameAndPassword.getUsername(), e.getMessage());
                }
            }
        }
        throw new ServerErrorException("No available rdb instance host: " + hostAndPorts);
    }

    protected String sqlOfShowTable() {
        return SQL_SHOW_TABLES;
    }

    protected String sqlOfShowDatabase() {
        return SQL_SHOW_DATABASES;
    }

    protected String sqlOfDescTable(final String database, final String table) {
        return String.format(SQL_DESC_TABLE, database, table, database, table);
    }

    protected String generateMysqlUrl(final HostAndPort hostAndPort) {
        return generateMysqlUrl(hostAndPort, null);
    }

    protected String generateMysqlUrl(final HostAndPort hostAndPort, final String database) {
        return UrlUtil.generateJDBCUrl(DataSystemType.MYSQL.name().toLowerCase(), hostAndPort.getHost(), hostAndPort.getPort(), database, CONNECTION_PROPERTY);
    }

    protected Connection createConnection(
            final String url,
            final UsernameAndPassword usernameAndPassword) {
        try {
            return DriverManager.getConnection(
                    url,
                    usernameAndPassword.getUsername(),
                    usernameAndPassword.getPassword());
        } catch (SQLException e) {
            throw new ServerErrorException(String.format("Can not connect to %s with user %s", url, usernameAndPassword.getUsername()), e);
        }
    }

    protected void close(final Connection conn, final Statement stmt, final ResultSet rs) {
        if (Objects.nonNull(conn)) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        }
        if (Objects.nonNull(stmt)) {
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        }

        if (Objects.nonNull(rs)) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        }
    }
}
