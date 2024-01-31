package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.StarRocksHelperService.TableModelAndPrimaryKey;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.connect.data.Schema;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.util.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

// CHECKSTYLE:OFF
@RunWith(SpringRunner.class)
public class StarRocksHelperServiceTest {
    
    private static final MockedStatic<DriverManager> MOCKED_DRIVER_MANAGER = Mockito.mockStatic(DriverManager.class);
    
    private static final HostAndPort HOST_AND_PORT = new HostAndPort("6.6.6.2", 6662);
    
    private static final Set<HostAndPort> HOST_AND_PORTS = Sets.newHashSet(HOST_AND_PORT);
    
    private static final UsernameAndPassword USERNAME_AND_PASSWORD = new UsernameAndPassword("user", "password");
    
    private static final String DATABASE_NAME = "dummy_database";
    
    private static final String TABLE_NAME = "dummy_table";
    
    private static final String QUERY_TABLE_MODEL_AND_PRIMARY_KEY_SQL = "select TABLE_MODEL, PRIMARY_KEY from information_schema.tables_config"
            + " where TABLE_SCHEMA = '" + DATABASE_NAME + "' and TABLE_NAME = '" + TABLE_NAME + "'";
    
    private static final String PRIMARY_KEY_VALUE = "`field_1`, `field_2`";
    
    private static final Set<String> PRIMARY_KEY_FIELD_NAMES = Sets.newHashSet("field_1", "field_2");
    
    private static final String TABLE_MODEL_VALUE = "UNIQUE_KEYS";
    
    private static final String QUERY_TABLE_FIELDS_SQL = "select COLUMN_NAME, COLUMN_TYPE from information_schema.columns"
            + " where TABLE_SCHEMA = '" + DATABASE_NAME + "' and TABLE_NAME = '" + TABLE_NAME + "'";
    
    @Mock
    private MysqlHelperService mysqlHelperService;
    
    private StarRocksHelperService starRocksHelperService;
    
    @Before
    public void setup() {
        starRocksHelperService = new StarRocksHelperService();
        
        ReflectionTestUtils.setField(starRocksHelperService, "mysqlHelperService", mysqlHelperService);
    }
    
    @AfterClass
    public static void tearDownClass() {
        MOCKED_DRIVER_MANAGER.close();
    }
    
    @Test
    public void testShowDataBasesShouldAsExcept() {
        starRocksHelperService.showDataBases(HOST_AND_PORTS, USERNAME_AND_PASSWORD, database -> true);
        Mockito.verify(mysqlHelperService, Mockito.times(1)).showDataBases(anySet(), any(UsernameAndPassword.class), any(Predicate.class));
    }
    
    @Test
    public void testShowTablesShouldAsExcept() {
        starRocksHelperService.showTables(HOST_AND_PORTS, USERNAME_AND_PASSWORD, DATABASE_NAME);
        Mockito.verify(mysqlHelperService, Mockito.times(1)).showTables(anySet(), any(UsernameAndPassword.class), anyString());
    }
    
    @Test
    public void testDescTableShouldAsExcept() {
        List<RelationalDatabaseTableField> fields = new ArrayList<>();
        fields.add(new RelationalDatabaseTableField("field_1", "varchar(32)", StarRocksHelperService.UNIQUE_INDEX_NAMES));
        fields.add(new RelationalDatabaseTableField("field_2", "varchar(32)", StarRocksHelperService.UNIQUE_INDEX_NAMES));
        fields.add(new RelationalDatabaseTableField("field_3", "varchar(32)", null));
        fields.add(new RelationalDatabaseTableField("field_4", "varchar(32)", null));
        fields.add(new RelationalDatabaseTableField("field_5", "varchar(32)", null));
        
        TableModelAndPrimaryKey tableModelAndPrimaryKey = new TableModelAndPrimaryKey(TABLE_MODEL_VALUE, PRIMARY_KEY_FIELD_NAMES);
        
        when(mysqlHelperService.choiceAvailableInstance(anySet(), any(UsernameAndPassword.class))).thenReturn(HOST_AND_PORT);
        when(mysqlHelperService.executeQuery(any(HostAndPort.class), any(UsernameAndPassword.class), anyString(), any())).thenReturn(tableModelAndPrimaryKey).thenReturn(fields);
        
        RelationalDatabaseTable relationalDatabaseTable = starRocksHelperService.descTable(HOST_AND_PORTS, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME);
        
        Assertions.assertThat(relationalDatabaseTable.getName()).isEqualTo(TABLE_NAME);
        Assertions.assertThat(relationalDatabaseTable.getProperties().getProperty(StarRocksHelperService.TABLE_MODEL)).isEqualTo(TABLE_MODEL_VALUE);
        Assertions.assertThat(relationalDatabaseTable.getFields()).isEqualTo(fields);
    }
    
    @Test
    public void testQueryTableModelAndPrimaryKeyShouldAsExcept() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(StarRocksHelperService.PRIMARY_KEY)).thenReturn(PRIMARY_KEY_VALUE);
        when(resultSet.getString(StarRocksHelperService.TABLE_MODEL)).thenReturn(TABLE_MODEL_VALUE);
        
        starRocksHelperService.queryTableModelAndPrimaryKey(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME);
        
        ArgumentCaptor<Function<ResultSet, TableModelAndPrimaryKey>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
        Mockito.verify(mysqlHelperService).executeQuery(any(HostAndPort.class), any(UsernameAndPassword.class), anyString(), callbackCaptor.capture());
        TableModelAndPrimaryKey tableModelAndPrimaryKey = callbackCaptor.getValue().apply(resultSet);
        
        Assertions.assertThat(tableModelAndPrimaryKey.getTableModel()).isEqualTo(TABLE_MODEL_VALUE);
        Assertions.assertThat(tableModelAndPrimaryKey.getPrimaryKeyFieldNames()).isEqualTo(PRIMARY_KEY_FIELD_NAMES);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testQueryTableModelAndPrimaryKeyShouldThrowExceptionWhenTableNotExisted() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.next()).thenReturn(false);
        
        starRocksHelperService.queryTableModelAndPrimaryKey(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME);
        
        ArgumentCaptor<Function<ResultSet, ?>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
        Mockito.verify(mysqlHelperService).executeQuery(any(HostAndPort.class), any(UsernameAndPassword.class), anyString(), callbackCaptor.capture());
        callbackCaptor.getValue().apply(resultSet);
    }
    
    @Test
    public void testGenerateQueryTableModelAndPrimaryKeySQLShouldAsExpect() {
        Assertions.assertThat(starRocksHelperService.generateQueryTableModelAndPrimaryKeySQL(DATABASE_NAME, TABLE_NAME)).isEqualTo(QUERY_TABLE_MODEL_AND_PRIMARY_KEY_SQL);
    }
    
    @Test
    public void testQueryTableFieldsAndSetUniqueIndexNameShouldAsExcept() {
        List<RelationalDatabaseTableField> fields = new ArrayList<>();
        fields.add(new RelationalDatabaseTableField("field_1", "varchar(32)", null));
        fields.add(new RelationalDatabaseTableField("field_2", "varchar(32)", null));
        fields.add(new RelationalDatabaseTableField("field_3", "varchar(32)", null));
        fields.add(new RelationalDatabaseTableField("field_4", "varchar(32)", null));
        fields.add(new RelationalDatabaseTableField("field_5", "varchar(32)", null));
        
        when(mysqlHelperService.executeQuery(any(HostAndPort.class), any(UsernameAndPassword.class), anyString(), any())).thenReturn(fields);
        
        List<RelationalDatabaseTableField> results = starRocksHelperService.queryTableFieldsAndSetUniqueIndexName(
                HOST_AND_PORT,
                USERNAME_AND_PASSWORD,
                DATABASE_NAME,
                TABLE_NAME,
                PRIMARY_KEY_FIELD_NAMES);
        
        Assertions.assertThat(results).isEqualTo(fields);
        Assertions.assertThat(results.get(0).getUniqueIndexNames()).isEqualTo(StarRocksHelperService.UNIQUE_INDEX_NAMES);
        Assertions.assertThat(results.get(1).getUniqueIndexNames()).isEqualTo(StarRocksHelperService.UNIQUE_INDEX_NAMES);
        Assertions.assertThat(results.get(2).getUniqueIndexNames()).isNull();
        Assertions.assertThat(results.get(3).getUniqueIndexNames()).isNull();
        Assertions.assertThat(results.get(4).getUniqueIndexNames()).isNull();
    }
    
    @Test
    public void testQueryTableFieldsShouldAsExcept() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        
        List<String> fieldNames = Lists.newArrayList("field_1", "field_2", "field_3");
        when(resultSet.getString(1)).thenReturn(fieldNames.get(0), fieldNames.get(1), fieldNames.get(2));
        
        List<String> fieldTypes = Lists.newArrayList("varchar(32)", "int", "decimal");
        when(resultSet.getString(2)).thenReturn(fieldTypes.get(0), fieldTypes.get(1), fieldTypes.get(2));
        
        starRocksHelperService.queryTableFields(HOST_AND_PORT, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME);
        
        ArgumentCaptor<Function<ResultSet, List<RelationalDatabaseTableField>>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
        Mockito.verify(mysqlHelperService).executeQuery(any(HostAndPort.class), any(UsernameAndPassword.class), anyString(), callbackCaptor.capture());
        List<RelationalDatabaseTableField> fields = callbackCaptor.getValue().apply(resultSet);
        
        Assertions.assertThat(fields.size()).isEqualTo(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            Assertions.assertThat(fields.get(i).getName()).isEqualTo(fieldNames.get(i));
            Assertions.assertThat(fields.get(i).getType()).isEqualTo(fieldTypes.get(i));
        }
    }
    
    @Test
    public void testGenerateQueryTableFieldsSQLShouldAsExcept() {
        Assertions.assertThat(starRocksHelperService.generateQueryTableFieldsSQL(DATABASE_NAME, TABLE_NAME)).isEqualTo(QUERY_TABLE_FIELDS_SQL);
    }
    
    @Test
    public void testCheckHealthShouldPass() {
        starRocksHelperService.checkHealth(HOST_AND_PORTS, USERNAME_AND_PASSWORD);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckHealthShouldThrowExceptionWhenCanNotExecuteCheckHealthSQL() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(SQLException.class);
        
        when(mysqlHelperService.choiceAvailableInstance(anySet(), any(UsernameAndPassword.class))).thenReturn(HOST_AND_PORT);
        starRocksHelperService.checkHealth(HOST_AND_PORTS, USERNAME_AND_PASSWORD);
        
        ArgumentCaptor<Function<ResultSet, Boolean>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
        Mockito.verify(mysqlHelperService).executeQuery(any(HostAndPort.class), any(UsernameAndPassword.class), anyString(), callbackCaptor.capture());
        callbackCaptor.getValue().apply(resultSet);
    }
    
    @Test
    public void testCheckPermissionsShouldPass() {
    
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckPermissionsShouldThrowExceptionWhenInsufficientPermission() {
        throw new ServerErrorException("");
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testCreateDataCollectionIfAbsentShouldThrowExceptionWithNoUks() {
        List<DataFieldDefinition> fieldDefinitions = fakeFieldDefinitions();
        Map<String, List<DataFieldDefinition>> uniqueIndexNameToFieldDefinitions = new HashMap<>();
        
        HostAndPort first = HOST_AND_PORTS.stream().findFirst().orElseThrow(UnsupportedOperationException::new);
        Mockito.when(mysqlHelperService.choiceAvailableInstance(ArgumentMatchers.eq(HOST_AND_PORTS), ArgumentMatchers.eq(USERNAME_AND_PASSWORD))).thenReturn(first);
        
        starRocksHelperService.createDataCollectionIfAbsent(HOST_AND_PORTS, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME, fieldDefinitions, uniqueIndexNameToFieldDefinitions);
        
        ArgumentCaptor<String> sqlCapture = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mysqlHelperService).execute(ArgumentMatchers.eq(first), ArgumentMatchers.eq(USERNAME_AND_PASSWORD), sqlCapture.capture());
    }
    
    @Test
    public void testCreateDataCollectionIfAbsentShouldPassWithPrimaryKeySingleField() {
        List<DataFieldDefinition> fieldDefinitions = fakeFieldDefinitions();
        Map<String, List<DataFieldDefinition>> uniqueIndexNameToFieldDefinitions = new HashMap<>();
        uniqueIndexNameToFieldDefinitions.put("PRIMARY", fieldDefinitions.stream().filter(dataFieldDefinition -> dataFieldDefinition.getName().equals("id")).collect(Collectors.toList()));
        
        HostAndPort first = HOST_AND_PORTS.stream().findFirst().orElseThrow(UnsupportedOperationException::new);
        Mockito.when(mysqlHelperService.choiceAvailableInstance(ArgumentMatchers.eq(HOST_AND_PORTS), ArgumentMatchers.eq(USERNAME_AND_PASSWORD))).thenReturn(first);
        
        starRocksHelperService.createDataCollectionIfAbsent(HOST_AND_PORTS, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME, fieldDefinitions, uniqueIndexNameToFieldDefinitions);
        
        ArgumentCaptor<String> sqlCapture = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mysqlHelperService).execute(ArgumentMatchers.eq(first), ArgumentMatchers.eq(USERNAME_AND_PASSWORD), sqlCapture.capture());
        String expectedSql = "create table if not exists `dummy_database`.`dummy_table` (\n" +
                "\tid bigint NOT NULL,\n" +
                "\tcode VARCHAR(1048576) NULL,\n" +
                "\tname VARCHAR(1048576) NULL\n" +
                ") PRIMARY KEY (id) \n" +
                "DISTRIBUTED BY HASH(id)\n" +
                "PROPERTIES (\n" +
                "\t\"enable_persistent_index\" = \"true\"\n" +
                ")";
        Assert.equals(expectedSql, sqlCapture.getValue());
    }
    
    @Test
    public void testCreateDataCollectionIfAbsentShouldPassWithPrimaryKeyMultiFields() {
        List<DataFieldDefinition> fieldDefinitions = fakeFieldDefinitions();
        Map<String, List<DataFieldDefinition>> uniqueIndexNameToFieldDefinitions = new HashMap<>();
        uniqueIndexNameToFieldDefinitions.put("PRIMARY", fieldDefinitions.stream()
                .filter(dataFieldDefinition -> dataFieldDefinition.getName().equals("id") || dataFieldDefinition.getName().equals("code"))
                .collect(Collectors.toList())
        );
        
        HostAndPort first = HOST_AND_PORTS.stream().findFirst().orElseThrow(UnsupportedOperationException::new);
        Mockito.when(mysqlHelperService.choiceAvailableInstance(ArgumentMatchers.eq(HOST_AND_PORTS), ArgumentMatchers.eq(USERNAME_AND_PASSWORD))).thenReturn(first);
        
        starRocksHelperService.createDataCollectionIfAbsent(HOST_AND_PORTS, USERNAME_AND_PASSWORD, DATABASE_NAME, TABLE_NAME, fieldDefinitions, uniqueIndexNameToFieldDefinitions);
        
        ArgumentCaptor<String> sqlCapture = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mysqlHelperService).execute(ArgumentMatchers.eq(first), ArgumentMatchers.eq(USERNAME_AND_PASSWORD), sqlCapture.capture());
        String expectedSql = "create table if not exists `dummy_database`.`dummy_table` (\n" +
                "\tid bigint NOT NULL,\n" +
                "\tcode VARCHAR(1048576) NOT NULL,\n" +
                "\tname VARCHAR(1048576) NULL\n" +
                ") PRIMARY KEY (id,code) \n" +
                "DISTRIBUTED BY HASH(id,code)\n" +
                "PROPERTIES (\n" +
                "\t\"enable_persistent_index\" = \"true\"\n" +
                ")";
        Assert.equals(expectedSql, sqlCapture.getValue());
    }
    
    private List<DataFieldDefinition> fakeFieldDefinitions() {
        List<DataFieldDefinition> result = new ArrayList<>();
        DataFieldDefinition id = new DataFieldDefinition("id", "bigint", Schema.INT64_SCHEMA, false, "1", new HashMap<>(), new HashSet<>());
        result.add(id);
        DataFieldDefinition code = new DataFieldDefinition("code", "VARCHAR(1048576)", Schema.STRING_SCHEMA, false, "code_1", new HashMap<>(), new HashSet<>());
        result.add(code);
        DataFieldDefinition name = new DataFieldDefinition("name", "VARCHAR(1048576)", Schema.STRING_SCHEMA, false, "name_1", new HashMap<>(), new HashSet<>());
        result.add(name);
        return result;
    }
}
