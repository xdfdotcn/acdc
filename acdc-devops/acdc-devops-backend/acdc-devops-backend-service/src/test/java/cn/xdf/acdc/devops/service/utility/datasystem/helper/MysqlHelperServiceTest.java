package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.config.RuntimeProperties;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.util.UrlUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MysqlHelperServiceTest {
    
    private static final List<String[]> PERMISSIONS_FOR_MASTER = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"INSERT"}, new String[]{"UPDATE"},
            new String[]{"DELETE"});
    
    private static final List<String[]> PERMISSIONS_FOR_DATASOURCE = Lists.newArrayList(new String[]{"SELECT"}, new String[]{"REPLICATION SLAVE"},
            new String[]{"REPLICATION CLIENT"}, new String[]{"RELOAD", "LOCK TABLES"});
    
    private static final HostAndPort HOST_AND_PORT = new HostAndPort("6.6.6.2", 6662);
    
    private static final UsernameAndPassword USERNAME_AND_PASSWORD = new UsernameAndPassword("user", "password");
    
    private static final String DATABASE_NAME = "dummy_database";
    
    private static final String TABLE_NAME = "dummy_table";
    
    private static final MockedStatic<DriverManager> MOCKED_DRIVER_MANAGER = Mockito.mockStatic(DriverManager.class);
    
    private MysqlHelperService mysqlHelperService;
    
    @Mock
    private RuntimeProperties runtimeProperties;
    
    @Mock
    private RuntimeProperties.Host host;
    
    @AfterClass
    public static void tearDownClass() {
        MOCKED_DRIVER_MANAGER.close();
    }
    
    @Before
    public void setup() {
        when(runtimeProperties.getHost()).thenReturn(host);
        when(host.getRanges()).thenReturn(Sets.newHashSet("%"));
        when(host.getIps()).thenReturn(Sets.newHashSet("%"));
        
        mysqlHelperService = new MysqlHelperService(runtimeProperties);
    }
    
    private ResultSet generateMockedResultSet() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        
        return resultSet;
    }
    
    @Test
    public void testDDLSQl() {
        Assertions.assertThat(mysqlHelperService.sqlOfShowTable()).isEqualTo(MysqlHelperService.SQL_SHOW_TABLES);
        
        Assertions.assertThat(mysqlHelperService.sqlOfShowDatabase()).isEqualTo(MysqlHelperService.SQL_SHOW_DATABASES);
        
        Assertions.assertThat(mysqlHelperService.sqlOfDescTable(DATABASE_NAME, TABLE_NAME))
                .isEqualTo(String.format(MysqlHelperService.SQL_DESC_TABLE, DATABASE_NAME, TABLE_NAME, DATABASE_NAME, TABLE_NAME));
        
        Assertions.assertThat(mysqlHelperService.generateMysqlUrl(HOST_AND_PORT))
                .isEqualTo(UrlUtil.generateJDBCUrl(DataSystemType.MYSQL.name().toLowerCase(), HOST_AND_PORT.getHost(), HOST_AND_PORT.getPort(), null, MysqlHelperService.CONNECTION_PROPERTY));
        
        Assertions.assertThat(mysqlHelperService.generateMysqlUrl(HOST_AND_PORT, DATABASE_NAME))
                .isEqualTo(UrlUtil.generateJDBCUrl(DataSystemType.MYSQL.name().toLowerCase(), HOST_AND_PORT.getHost(), HOST_AND_PORT.getPort(), DATABASE_NAME, MysqlHelperService.CONNECTION_PROPERTY));
    }
    
    @Test
    public void testShowDataBasesWithIpAndPort() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getString(1)).thenReturn("db_test");
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        
        List<String> databases = mysqlHelperService.showDataBases(HOST_AND_PORT, USERNAME_AND_PASSWORD);
        Assertions.assertThat(databases.size() == 1);
        Assertions.assertThat(databases.get(0).equals("db_test"));
        
        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();
        
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        Assertions.assertThat(sqlCaptor.getValue()).isEqualTo(MysqlHelperService.SQL_SHOW_DATABASES);
        
    }
    
    @Test
    public void testShowDataBasesWithIpAndPortAndPredicate() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getString(1)).thenReturn("db_test1").thenReturn("db_test2");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        
        List<String> databases = mysqlHelperService.showDataBases(HOST_AND_PORT, USERNAME_AND_PASSWORD, database -> !database.equals("db_test2"));
        Assertions.assertThat(databases.size() == 1);
        Assertions.assertThat(databases.get(0).equals("db_test1"));
        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();
    }
    
    @Test
    public void testShowDataBaseShouldGetEmptyWhenNotExist() throws SQLException {
        ResultSet resultSet = generateMockedResultSet();
        when(resultSet.next()).thenReturn(false);
        
        List<String> databases = mysqlHelperService.showDataBases(HOST_AND_PORT, USERNAME_AND_PASSWORD, database -> !database.equals("db_test2"));
        Assertions.assertThat(databases.isEmpty()).isEqualTo(true);
    }
    
    @Test
    public void testShowDataBasesShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases(new HostAndPort("", 6662), USERNAME_AND_PASSWORD));
        Assertions.assertThat(throwable1).isInstanceOf(IllegalArgumentException.class);
        
        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases(new HostAndPort("6.6", 0), USERNAME_AND_PASSWORD));
        Assertions.assertThat(throwable2).isInstanceOf(IllegalArgumentException.class);
    }
    
    @Test
    public void testShowTables() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        
        when(resultSet.getString(1)).thenReturn("table1").thenReturn("table2");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        
        List<String> tables = mysqlHelperService.showTables(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME);
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
        ResultSet resultSet = generateMockedResultSet();
        when(resultSet.next()).thenReturn(false);
        List<String> tables = mysqlHelperService.showTables(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME);
        Assertions.assertThat(tables.isEmpty()).isEqualTo(true);
    }
    
    @Test
    public void testDescTable() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        
        when(resultSet.getString(1)).thenReturn("id").thenReturn("column_2").thenReturn("column_3").thenReturn("column_3").thenReturn("column_4").thenReturn("column_5");
        when(resultSet.getString(2)).thenReturn("bigint(20)").thenReturn("varchar(32)").thenReturn("varchar(128)").thenReturn("varchar(128)").thenReturn("varchar(32)").thenReturn("varchar(32)");
        when(resultSet.getString(3)).thenReturn("PRIMARY").thenReturn("unique_index_1").thenReturn("unique_index_2").thenReturn("multi_unique_index_1").thenReturn("multi_unique_index_1")
                .thenReturn(null);
        
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        
        final List<RelationalDatabaseTableField> fields = mysqlHelperService.descTable(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME);
        
        List<RelationalDatabaseTableField> expectedFields = new ArrayList<>();
        expectedFields.add(new RelationalDatabaseTableField().setName("id").setType("bigint(20)").setUniqueIndexNames(Sets.newHashSet("PRIMARY")));
        expectedFields.add(new RelationalDatabaseTableField().setName("column_2").setType("varchar(32)").setUniqueIndexNames(Sets.newHashSet("unique_index_1")));
        expectedFields.add(new RelationalDatabaseTableField().setName("column_3").setType("varchar(128)").setUniqueIndexNames(Sets.newHashSet("unique_index_2", "multi_unique_index_1")));
        expectedFields.add(new RelationalDatabaseTableField().setName("column_4").setType("varchar(32)").setUniqueIndexNames(Sets.newHashSet("multi_unique_index_1")));
        expectedFields.add(new RelationalDatabaseTableField().setName("column_5").setType("varchar(32)").setUniqueIndexNames(new HashSet<>()));
        
        Collections.sort(fields, Comparator.comparing(RelationalDatabaseTableField::getName));
        Collections.sort(expectedFields, Comparator.comparing(RelationalDatabaseTableField::getName));
        
        Assertions.assertThat(fields).isEqualTo(expectedFields);
        
        Mockito.verify(connection, Mockito.times(1)).close();
        Mockito.verify(statement, Mockito.times(1)).close();
        Mockito.verify(resultSet, Mockito.times(1)).close();
    }
    
    @Test(expected = ServerErrorException.class)
    public void testDescTableShouldThrowExceptionWhenNotExist() throws SQLException {
        ResultSet resultSet = generateMockedResultSet();
        when(resultSet.next()).thenReturn(false);
        mysqlHelperService.descTable(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME);
    }
    
    @Test
    public void testDescTableShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.descTable(new HostAndPort("", 6662), USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME));
        Assertions.assertThat(throwable1).isInstanceOf(IllegalArgumentException.class);
        
        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.descTable(new HostAndPort("6.6.6.6", 0), USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME));
        Assertions.assertThat(throwable2).isInstanceOf(IllegalArgumentException.class);
        
        Throwable throwable3 = Assertions.catchThrowable(() -> mysqlHelperService.descTable(HOST_AND_PORT, USERNAME_AND_PASSWORD, "", TABLE_NAME));
        Assertions.assertThat(throwable3).isInstanceOf(IllegalArgumentException.class);
        
        Throwable throwable4 = Assertions.catchThrowable(() -> mysqlHelperService.descTable(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, ""));
        Assertions.assertThat(throwable4).isInstanceOf(IllegalArgumentException.class);
    }
    
    @Test
    public void testShowTablesShouldThrowExceptionWhenGivenIllegalParameter() {
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.showTables(new HostAndPort("", 6662), USERNAME_AND_PASSWORD, DATABASE_NAME));
        Assertions.assertThat(throwable1).isInstanceOf(IllegalArgumentException.class);
        
        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.showTables(new HostAndPort("6.6.6.6", 0), USERNAME_AND_PASSWORD, DATABASE_NAME));
        Assertions.assertThat(throwable2).isInstanceOf(IllegalArgumentException.class);
        
        Throwable throwable3 = Assertions.catchThrowable(() -> mysqlHelperService.showTables(HOST_AND_PORT, USERNAME_AND_PASSWORD, ""));
        Assertions.assertThat(throwable3).isInstanceOf(IllegalArgumentException.class);
    }
    
    @Test
    public void testShouldThrowExceptionWhenDbException() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        
        // mock db exception
        doThrow(new SQLException()).when(resultSet).next();
        
        Throwable throwable1 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases(HOST_AND_PORT, USERNAME_AND_PASSWORD));
        Assertions.assertThat(throwable1).isInstanceOf(RuntimeException.class);
        
        Throwable throwable2 = Assertions.catchThrowable(() -> mysqlHelperService.showDataBases(HOST_AND_PORT, USERNAME_AND_PASSWORD, db -> false));
        Assertions.assertThat(throwable2).isInstanceOf(RuntimeException.class);
        
        Throwable throwable3 = Assertions.catchThrowable(() -> mysqlHelperService.showTables(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME));
        Assertions.assertThat(throwable3).isInstanceOf(RuntimeException.class);
        
        Throwable throwable4 = Assertions.catchThrowable(() -> mysqlHelperService.descTable(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME));
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
        Assertions.assertThat(grantedPermissions.isEmpty());
    }
    
    @Test
    public void testCheckPermissionsForDataSourceShouldPass() {
        Set<String> grantedPermissionsForDataSource = Sets.newHashSet("SELECT", "REPLICATION SLAVE", "LOCK TABLES", "REPLICATION CLIENT");
        mysqlHelperService.checkPermissions(grantedPermissionsForDataSource, PERMISSIONS_FOR_DATASOURCE);
    }
    
    @Test
    public void testCheckPermissionsForMaster() {
        Set<String> grantedPermissionsForMaster = Sets.newHashSet("SELECT", "INSERT", "UPDATE", "DELETE");
        mysqlHelperService.checkPermissions(grantedPermissionsForMaster, PERMISSIONS_FOR_MASTER);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenInsufficientPermission() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        mockShowGrantsPrepareStatement(connection, statement, resultSet, USERNAME_AND_PASSWORD.getUsername());
        when(resultSet.getString(1)).thenReturn("GRANT SELECT ON *.* TO 'username'@'%'");
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        
        mysqlHelperService.checkPermissions(HOST_AND_PORT, USERNAME_AND_PASSWORD, PERMISSIONS_FOR_DATASOURCE);
    }
    
    @Test
    public void testCheckGrants() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        mockShowGrantsPrepareStatement(connection, statement, resultSet, USERNAME_AND_PASSWORD.getUsername());
        when(resultSet.getString(1))
                .thenReturn("GRANT SELECT, RELOAD,REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username'@'%'")
                .thenReturn("GRANT Update,Delete,Insert ON *.* TO 'username'@'%'");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        
        mysqlHelperService.checkPermissions(HOST_AND_PORT, USERNAME_AND_PASSWORD, PERMISSIONS_FOR_DATASOURCE);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testShouldThrowExceptionWhenShowGrantsResultIsUnexpected() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        mockShowGrantsPrepareStatement(connection, statement, resultSet, USERNAME_AND_PASSWORD.getUsername());
        when(resultSet.getString(1)).thenReturn("Unexpect show grant result");
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        
        mysqlHelperService.checkPermissions(HOST_AND_PORT, USERNAME_AND_PASSWORD, PERMISSIONS_FOR_DATASOURCE);
    }
    
    private void mockShowGrantsPrepareStatement(final Connection connection, final PreparedStatement statement, final ResultSet resultSet, final String username) throws SQLException {
        when(connection.prepareStatement(String.format("show grants for %s@'%s'", username, "%"))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
    }
    
    @Test
    public void testShowVariablesShouldAsExcept() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(MysqlHelperService.SHOW_VARIABLES_SQL)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getString(1)).thenReturn("log_bin").thenReturn("binlog_format").thenReturn("binlog_row_image").thenReturn("expire_logs_days");
        when(resultSet.getString(2)).thenReturn("ON").thenReturn("ROW").thenReturn("FULL").thenReturn("4");
        
        Map<String, String> variables = new HashMap<>();
        variables.put("log_bin", "ON");
        variables.put("binlog_format", "ROW");
        variables.put("binlog_row_image", "FULL");
        variables.put("expire_logs_days", "4");
        
        Map<String, String> actualVariables = mysqlHelperService.showVariables(HOST_AND_PORT, USERNAME_AND_PASSWORD);
        Assertions.assertThat(variables).isEqualTo(actualVariables);
    }
}
