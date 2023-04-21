package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.service.config.HiveJdbcProperties;
import cn.xdf.acdc.devops.service.config.RuntimeProperties;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class HiveHelperServiceTest {

    private static final MockedStatic<DriverManager> MOCKED_DRIVER_MANAGER = Mockito.mockStatic(DriverManager.class);

    @Mock
    private HiveJdbcProperties config;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement statement;

    @Mock
    private ResultSet resultSet;

    private HiveHelperService hiveHelperService;

    @AfterClass
    public static void tearDownClass() {
        MOCKED_DRIVER_MANAGER.close();
    }

    @Before
    public void setup() throws SQLException {
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);

        when(config.getUser()).thenReturn("");
        when(config.getPassword()).thenReturn("");
        when(config.getUrl()).thenReturn("");

        hiveHelperService = new HiveHelperService();
        ReflectionTestUtils.setField(hiveHelperService, "config", config);
        ReflectionTestUtils.setField(hiveHelperService, "mysqlHelperService", new MysqlHelperService(new RuntimeProperties()));
    }

    @Test
    public void testShowTables() throws SQLException {
        String database = "db";
        when(resultSet.getString(1)).thenReturn("tb1").thenReturn("tb2").thenReturn("tb3");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);

        List<String> expectedTables = Lists.newArrayList(
                "tb1",
                "tb2",
                "tb3"
        );

        List<String> actualTables = hiveHelperService.showTables(database);
        Mockito.verify(connection).prepareStatement(eq(getFieldValueToString("SQL_SHOW_TABLES")));
        Assertions.assertThat(actualTables).containsAll(expectedTables);
        Mockito.verify(statement).setString(eq(1), eq(database));
    }

    @Test
    public void testShowTablesShouldReturnEmptyWhenNotExistTable() throws SQLException {
        String database = "db";
        when(resultSet.next()).thenReturn(false);

        List<String> actualTables = hiveHelperService.showTables(database);
        Assertions.assertThat(actualTables.size()).isEqualTo(0);
    }

    @Test
    public void testShowDatabases() throws SQLException {
        when(resultSet.getString(1)).thenReturn("db1").thenReturn("db2").thenReturn("db3");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        List<String> expectedDatabases = Lists.newArrayList(
                "db1",
                "db2",
                "db3"
        );
        List<String> actualDatabases = hiveHelperService.showDatabases();
        Mockito.verify(connection).prepareStatement(eq(getFieldValueToString("SQL_SHOW_DATABASES")));
        Assertions.assertThat(actualDatabases).containsAll(expectedDatabases);
    }

    @Test
    public void testShowDatabasesShouldReturnEmptyWhenNotExistDatabase() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        List<String> actualDatabases = hiveHelperService.showDatabases();
        Assertions.assertThat(actualDatabases.size()).isEqualTo(0);
    }

    @Test
    public void testDescTable() throws SQLException {
        String database = "db";
        String table = "tb";
        // mock jdbc result
        when(resultSet.getString(1)).thenReturn("id").thenReturn("column_2").thenReturn("column_3");
        when(resultSet.getString(2)).thenReturn("bigint(20)").thenReturn("varchar(32)").thenReturn("varchar(128)");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);

        // execute target method
        List<RelationalDatabaseTableField> fields = hiveHelperService.descTable(database, table);

        // verify result
        List<RelationalDatabaseTableField> expectedFields = new ArrayList<>();
        expectedFields.add(RelationalDatabaseTableField.builder().name("id").type("bigint(20)").build());
        expectedFields.add(RelationalDatabaseTableField.builder().name("column_2").type("varchar(32)").build());
        expectedFields.add(RelationalDatabaseTableField.builder().name("column_3").type("varchar(128)").build());

        Assertions.assertThat(fields).isEqualTo(expectedFields);

        // verify sql
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        Assertions.assertThat(sqlCaptor.getValue()).isEqualTo(getFieldValueToString("SQL_DESC_TABLE"));

        // verify sql params
        Mockito.verify(statement).setString(eq(1), eq(database));
        Mockito.verify(statement).setString(eq(2), eq(table));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDescTableShouldThrowExceptionWhenFieldsIsEmpty() throws SQLException {
        String database = "db";
        String table = "tb";
        // mock jdbc result
        when(resultSet.next()).thenReturn(false);
        hiveHelperService.descTable(database, table);
    }

    private String getFieldValueToString(final String fieldName) {
        return String.valueOf(ReflectionTestUtils.getField(hiveHelperService, fieldName));
    }
}
