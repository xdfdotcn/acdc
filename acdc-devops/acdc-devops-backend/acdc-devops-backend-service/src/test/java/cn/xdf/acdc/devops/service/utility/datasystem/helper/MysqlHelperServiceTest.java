package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.service.config.RuntimeConfig;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

// CHECKSTYLE:OFF

@RunWith(MockitoJUnitRunner.class)
public class MysqlHelperServiceTest {

    private static final List<String[]> PERMISSIONS_FOR_MASTER = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"INSERT"}, new String[]{"UPDATE"},
            new String[]{"DELETE"});

    private static final List<String[]> PERMISSIONS_FOR_DATASOURCE = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"REPLICATION SLAVE"},
            new String[]{"REPLICATION CLIENT"}, new String[]{"RELOAD", "LOCK TABLES"});

    private static final String QUERY_LOG_BIN_SQL = "show variables like 'log_bin'";

    private static final String QUERY_BINLOG_FORMAT_SQL = "show variables like 'binlog_format'";

    private static final String QUERY_BINLOG_ROW_IMAGE_SQL = "show variables like 'binlog_row_image'";

    private static final String QUERY_EXPIRE_LOGS_DAYS_SQL = "show variables like 'expire_logs_days'";

    private static final String QUERY_SQL_MODE_SQL = "show variables like 'sql_mode'";

    private MysqlHelperService mysqlHelperService;

    @Mock
    private RuntimeConfig runtimeConfig;

    @Mock
    private RuntimeConfig.Host host;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement statement;

    @Mock
    private PreparedStatement queryGrantsStatement;

    @Mock
    private PreparedStatement queryLogBinStatement;

    @Mock
    private PreparedStatement queryBinLogFormatStatement;

    @Mock
    private PreparedStatement queryBinLogRowImageStatement;

    @Mock
    private PreparedStatement queryBinLogExpireLogsDaysStatement;

    @Mock
    private PreparedStatement querySqlModeStatement;

    @Mock
    private ResultSet queryGrantsResultSet;

    @Mock
    private ResultSet queryLogBinResultSet;

    @Mock
    private ResultSet queryBinLogFormatResultSet;

    @Mock
    private ResultSet queryBinLogRowImageResultSet;

    @Mock
    private ResultSet queryBinLogExpireLogsDaysResultSet;

    @Mock
    private ResultSet querySqlModeResultSet;

    @Mock
    private ResultSet resultSet;

    @BeforeClass
    public static void init() {
        try {
            Mockito.mockStatic(DriverManager.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() throws SQLException {
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getString(1)).thenReturn("db_test");
        when(resultSet.next()).thenReturn(true).thenReturn(false);

        when(runtimeConfig.getHost()).thenReturn(host);
        when(host.getRanges()).thenReturn(Sets.newHashSet("%"));
        when(host.getIps()).thenReturn(Sets.newHashSet("%"));

        mysqlHelperService = new MysqlHelperService();
        mysqlHelperService.runtimeConfig = runtimeConfig;
    }

    @Test
    public void testDDLSQl() {
        Assertions.assertThat(mysqlHelperService.sqlOfShowTable()).isEqualTo(" SHOW TABLES ");

        Assertions.assertThat(mysqlHelperService.sqlOfShowDatabase()).isEqualTo(" SHOW DATABASES ");

        Assertions.assertThat(mysqlHelperService.sqlOfDescTable("test_01")).isEqualTo(" DESC `test_01` ");

        Assertions.assertThat(mysqlHelperService.urlOfRdbInstance("192.168.110.1", 8080))
                .isEqualTo("jdbc:mysql://192.168.110.1:8080");

        Assertions.assertThat(mysqlHelperService.urlOfRdbDatabase("192.168.110.1", 8080, "db_test"))
                .isEqualTo("jdbc:mysql://192.168.110.1:8080/db_test");
    }

    @Test
    public void testShowDataBasesWithIpAndPort() throws SQLException {
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getString(1)).thenReturn("db_test");
        when(resultSet.next()).thenReturn(true).thenReturn(false);

        List<String> databases = mysqlHelperService.showDataBases("192.168.110.1", 8080, "user", "password");
        assertTrue(databases.size() == 1);
        assertTrue(databases.get(0).equals("db_test"));

        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        Assertions.assertThat(sqlCaptor.getValue()).isEqualTo(" SHOW DATABASES ");

    }

    @Test
    public void testShowDataBasesWithIpAndPortAndPredicate() throws SQLException {
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
//        when(connection.createStatement()).thenReturn(statement);
//        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.getString(1)).thenReturn("db_test1").thenReturn("db_test2");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        List<String> databases = mysqlHelperService.showDataBases("192.168.110.1", 8080, "user", "password", database -> !database.equals("db_test2"));
        assertTrue(databases.size() == 1);
        assertTrue(databases.get(0).equals("db_test1"));
        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();
    }

    @Test
    public void testShowDataBaseShouldGetEmptyWhenNotExist() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        List<String> databases = mysqlHelperService.showDataBases("192.168.110.1", 8080, "user", "password", database -> !database.equals("db_test2"));
        Assertions.assertThat(databases.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testShowDataBasesShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases("", 8080, "user", "password"));
        Assertions.assertThat(throwable1).isInstanceOf(IllegalArgumentException.class);

        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases("192.168", 0, "user", "password"));
        Assertions.assertThat(throwable2).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testShowTables() throws SQLException {
        when(resultSet.getString(1)).thenReturn("table1").thenReturn("table2");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

        List<String> tables = mysqlHelperService.showTables("192.168.110.1", 8080, "user", "password", "db_test");
        Assertions.assertThat(tables.size()).isEqualTo(2);
        Assertions.assertThat(tables.get(1)).isEqualTo("table2");
        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        Assertions.assertThat(sqlCaptor.getValue()).isEqualTo(" SHOW TABLES ");
    }

    @Test
    public void testShowTablesShouldGetEmptyWhenNotExist() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        List<String> tables = mysqlHelperService.showTables("192.168.110.1", 8080, "user", "password", "db_test");
        Assertions.assertThat(tables.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testDescTable() throws SQLException {
        when(resultSet.getString(1)).thenReturn("id").thenReturn("");
        when(resultSet.getString(2)).thenReturn("bigint(20)").thenReturn("");
        when(resultSet.getString(3)).thenReturn("NO").thenReturn("");
        when(resultSet.getString(4)).thenReturn("PRI").thenReturn("");
        when(resultSet.getString(5)).thenReturn("12").thenReturn("");
        when(resultSet.getString(6)).thenReturn("auto_increment").thenReturn("");

        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        List<FieldDTO> fields = mysqlHelperService.descTable("192.168.110.1", 8900, "user", "password", "db_test", "table_test");

        Assertions.assertThat(fields.size()).isEqualTo(2);
        Assertions.assertThat(fields.get(0).getName()).isEqualTo("id");
        Assertions.assertThat(fields.get(0).getAllowNull()).isEqualTo("NO");
        Assertions.assertThat(fields.get(0).getDataType()).isEqualTo("bigint(20)");
        Assertions.assertThat(fields.get(0).getDefaultValue()).isEqualTo("12");
        Assertions.assertThat(fields.get(0).getKeyType()).isEqualTo("PRI");
        Assertions.assertThat(fields.get(0).getExtra()).isEqualTo("auto_increment");

        Assertions.assertThat(fields.get(1).getName()).isEqualTo("");
        Assertions.assertThat(fields.get(1).getAllowNull()).isEqualTo("");
        Assertions.assertThat(fields.get(1).getDataType()).isEqualTo("");
        Assertions.assertThat(fields.get(1).getKeyType()).isEqualTo("");
        Assertions.assertThat(fields.get(1).getDefaultValue()).isEqualTo("");

        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();
    }

    @Test
    public void testDescTableShouldThrowExceptionWhenNotExist() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.descTable("192.168.110.1", 8900, "user", "password", "db_test", "table_test"));
        Assertions.assertThat(throwable1).isInstanceOf(NotFoundException.class);
    }

    @Test
    public void testDescTableShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.descTable("", 8080, "user", "password", "db_test", "tb_test"));
        Assertions.assertThat(throwable1).isInstanceOf(IllegalArgumentException.class);

        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.descTable("192", 0, "user", "password", "db_test", "tb_test"));
        Assertions.assertThat(throwable2).isInstanceOf(IllegalArgumentException.class);

        Throwable throwable3 = Assertions.catchThrowable(() -> mysqlHelperService.descTable("192", 0, "user", "password", "", "tb_test"));
        Assertions.assertThat(throwable3).isInstanceOf(IllegalArgumentException.class);

        Throwable throwable4 = Assertions.catchThrowable(() -> mysqlHelperService.descTable("192", 0, "user", "password", "db_test", ""));
        Assertions.assertThat(throwable4).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testShowTablesShouldThrowExceptionWhenGivenIllegalParameter() {

        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.showTables("", 8080, "user", "password", "db"));
        Assertions.assertThat(throwable1).isInstanceOf(IllegalArgumentException.class);

        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.showTables("192", 0, "user", "password", "db"));
        Assertions.assertThat(throwable2).isInstanceOf(IllegalArgumentException.class);

        Throwable throwable3 = Assertions.catchThrowable(() -> mysqlHelperService.showTables("192", 8080, "user", "password", ""));
        Assertions.assertThat(throwable3).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testShouldThrowExceptionWhenDbException() throws SQLException {
        // mock db exception
        doThrow(new SQLException()).when(resultSet).next();

        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases("192", 8080, "user", "password"));
        Assertions.assertThat(throwable1).isInstanceOf(RuntimeException.class);

        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases("192", 8080, "user", "password", db -> false));
        Assertions.assertThat(throwable2).isInstanceOf(RuntimeException.class);

        Throwable throwable3 = Assertions.catchThrowable(() -> mysqlHelperService.showTables("192", 8080, "user", "password", "db"));
        Assertions.assertThat(throwable3).isInstanceOf(RuntimeException.class);

        Throwable throwable4 = Assertions.catchThrowable(() -> mysqlHelperService.descTable("192", 8080, "user", "password", "db", "tb"));
        Assertions.assertThat(throwable4).isInstanceOf(RuntimeException.class);

        Mockito.verify(connection, Mockito.times(4)).close();
        Mockito.verify(statement, Mockito.times(4)).close();
        Mockito.verify(resultSet, Mockito.times(4)).close();
    }

    @Test
    public void testShouldPassWhenLowerCase() {
        Set<String> grantedPermissions = mysqlHelperService
                .extractPermissionsFromShowGrantsResult("GRANT select, insErt, Update, delete,reload,REPLICATION slave, REPLICATION client ON *.* TO 'root'@'%'");
        mysqlHelperService.checkPermissions(grantedPermissions, PERMISSIONS_FOR_DATASOURCE);
    }

    @Test
    public void testShouldEmptyWhenGrantedPermissionsStringIsUnexpected() {
        Set<String> grantedPermissions = mysqlHelperService.extractPermissionsFromShowGrantsResult("unexpect show grant result.");
        assertTrue(grantedPermissions.isEmpty());
    }

    @Test
    public void testCheckPermissionsForDataSource() {
        Set<String> grantedPermissionsForDataSource = Sets.newHashSet("SELECT", "REPLICATION SLAVE", "LOCK TABLES", "REPLICATION CLIENT");
        mysqlHelperService.checkPermissions(grantedPermissionsForDataSource, PERMISSIONS_FOR_DATASOURCE);
    }

    @Test
    public void testCheckPermissionsForMaster() {
        Set<String> grantedPermissionsForMaster = Sets.newHashSet("SELECT", "INSERT", "UPDATE", "DELETE");
        mysqlHelperService.checkPermissions(grantedPermissionsForMaster, PERMISSIONS_FOR_MASTER);
    }

    @Test
    public void testCheckUserPermissionsAndBinlogConfiguration() throws SQLException {
        RdbInstanceDO rdbInstance = new RdbInstanceDO();
        rdbInstance.setRole(RoleType.DATA_SOURCE);
        rdbInstance.setPort(3306);
        rdbInstance.setHost("host");
        mockSufficientPermissionResultSet("username");
        mockQueryBinlogConfig(false, false, false, false);
        mysqlHelperService.checkUserPermissionsAndBinlogConfiguration(Lists.newArrayList(rdbInstance), "username", "password");
    }

    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenInsufficientPermission() throws SQLException {
        mockInsufficientPermissionResultSet("username");
        mysqlHelperService.checkPermissions("host", 3306, "username", "password", PERMISSIONS_FOR_DATASOURCE);
    }

    @Test
    public void testCheckGrants() throws SQLException {
        String username = "username";
        String password = "password";
        mockSufficientPermissionResultSet(username);
        mysqlHelperService.checkPermissions("host", 3306, username, password, PERMISSIONS_FOR_DATASOURCE);
    }

    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenShowGrantsResultIsUnexpected() throws SQLException {
        String username = "username";
        String password = "password";
        mockUnexpectedPermissionResultSet(username);
        mysqlHelperService.checkPermissions("host", 3306, username, password, PERMISSIONS_FOR_DATASOURCE);
    }

    private void mockSufficientPermissionResultSet(final String username) throws SQLException {
        mockPrepareStatement(username);
        when(queryGrantsResultSet.getString(1))
                .thenReturn("GRANT SELECT, RELOAD,REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username'@'%'")
                .thenReturn("GRANT Update,Delete,Insert ON *.* TO 'username'@'%'");
        when(queryGrantsResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    }

    private void mockInsufficientPermissionResultSet(final String username) throws SQLException {
        mockPrepareStatement(username);
        when(queryGrantsResultSet.getString(1)).thenReturn("GRANT SELECT ON *.* TO 'username'@'%'");
        when(queryGrantsResultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void mockUnexpectedPermissionResultSet(final String username) throws SQLException {
        mockPrepareStatement(username);
        when(queryGrantsResultSet.getString(1)).thenReturn("Unexpect show grant result");
        when(queryGrantsResultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void mockPrepareStatement(final String username) throws SQLException {
        when(connection.prepareStatement(String.format("show grants for %s@'%s'", username, "%"))).thenReturn(queryGrantsStatement);
        when(queryGrantsStatement.executeQuery()).thenReturn(queryGrantsResultSet);
    }

    @Test
    public void testCheckBinlogConfig() throws SQLException {
        mockQueryBinlogConfig(false, false, false, false);
        mysqlHelperService.checkBinlogConfiguration("host", 3306, "user", "pwd");
    }

    @Test
    public void testShouldPassWhenBinLogExpireTimeIsZero() throws SQLException {
        mockQueryMySqlConfig(false, QUERY_LOG_BIN_SQL, queryLogBinStatement, queryLogBinResultSet, "ON", "OFF");
        mockQueryMySqlConfig(false, QUERY_BINLOG_FORMAT_SQL, queryBinLogFormatStatement, queryBinLogFormatResultSet, "ROW", "");
        mockQueryMySqlConfig(false, QUERY_BINLOG_ROW_IMAGE_SQL, queryBinLogRowImageStatement, queryBinLogRowImageResultSet, "FULL", "");
        mockQueryMySqlConfig(false, QUERY_EXPIRE_LOGS_DAYS_SQL, queryBinLogExpireLogsDaysStatement, queryBinLogExpireLogsDaysResultSet, "0", "3");
        mysqlHelperService.checkBinlogConfiguration("host", 3306, "user", "pwd");
    }

    @Test
    public void testCheckSqlMode() throws SQLException {
        mockQueryMySqlConfig(false, QUERY_SQL_MODE_SQL, querySqlModeStatement, querySqlModeResultSet, "STRICT_TRANS_TABLES", "");
        mysqlHelperService.checkSqlMode("host", 3306, "user", "pwd");
    }

    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenSqlModeUnexcepted() throws SQLException {
        mockQueryMySqlConfig(true, QUERY_SQL_MODE_SQL, querySqlModeStatement, querySqlModeResultSet, "STRICT_TRANS_TABLES", "");
        mysqlHelperService.checkSqlMode("host", 3306, "user", "pwd");
    }

    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenLogBinConfigUnexpected() throws SQLException {
        mockQueryBinlogConfig(true, false, false, false);
        mysqlHelperService.checkBinlogConfiguration("host", 3306, "user", "pwd");
    }

    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenBinLogFormatConfigUnexpected() throws SQLException {
        mockQueryBinlogConfig(false, true, false, false);
        mysqlHelperService.checkBinlogConfiguration("host", 3306, "user", "pwd");
    }

    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenBinLogRowImageConfigUnexpected() throws SQLException {
        mockQueryBinlogConfig(false, false, true, false);
        mysqlHelperService.checkBinlogConfiguration("host", 3306, "user", "pwd");
    }


    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenExpireLogsDaysConfigUnexpected() throws SQLException {
        mockQueryBinlogConfig(false, false, false, true);
        mysqlHelperService.checkBinlogConfiguration("host", 3306, "user", "pwd");
    }

    private void mockQueryBinlogConfig(final Boolean isCheckLogBinException, final Boolean isCheckBinLogFormatException,
                                       final Boolean isCheckBinLogRowImageException, final Boolean isCheckExpireLogsDaysException) throws SQLException {
        mockQueryMySqlConfig(isCheckLogBinException, QUERY_LOG_BIN_SQL, queryLogBinStatement, queryLogBinResultSet, "ON", "OFF");
        mockQueryMySqlConfig(isCheckBinLogFormatException, QUERY_BINLOG_FORMAT_SQL, queryBinLogFormatStatement, queryBinLogFormatResultSet, "ROW", "");
        mockQueryMySqlConfig(isCheckBinLogRowImageException, QUERY_BINLOG_ROW_IMAGE_SQL, queryBinLogRowImageStatement, queryBinLogRowImageResultSet, "FULL", "");
        mockQueryMySqlConfig(isCheckExpireLogsDaysException, QUERY_EXPIRE_LOGS_DAYS_SQL, queryBinLogExpireLogsDaysStatement, queryBinLogExpireLogsDaysResultSet, "4", "3");
    }

    private void mockQueryMySqlConfig(Boolean isCheckException, String sql, PreparedStatement ps, ResultSet rs, String exceptedValue, String wrongValue) throws SQLException {
        when(connection.prepareStatement(sql)).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        if (isCheckException) {
            when(rs.getString(2)).thenReturn(wrongValue);
        } else {
            when(rs.getString(2)).thenReturn(exceptedValue);
        }
        when(rs.next()).thenReturn(true).thenReturn(false);
    }
}
