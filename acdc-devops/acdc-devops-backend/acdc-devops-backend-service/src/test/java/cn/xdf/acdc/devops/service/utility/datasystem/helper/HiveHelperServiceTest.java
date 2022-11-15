package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.HiveDbMetaDTO;
import cn.xdf.acdc.devops.service.config.HiveJdbcConfig;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HiveHelperServiceTest {

    @Mock
    private HiveJdbcConfig config;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement statement;

    @Mock
    private ResultSet resultSet;

    private HiveHelperService hiveHelperService;

    // CHECKSTYLE:OFF
    @BeforeClass
    public static void init() {
        try {
            Mockito.mockStatic(DriverManager.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // CHECKSTYLE:ON

    @Before
    public void setup() throws SQLException {
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(config.getUser()).thenReturn("");
        when(config.getPassword()).thenReturn("");
        when(config.getUrl()).thenReturn("");

        hiveHelperService = new HiveHelperService();
        ReflectionTestUtils.setField(hiveHelperService, "config", config);
    }

    @Test
    public void testFetchHiveDbMeta() throws SQLException {
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getLong(1)).thenReturn(1L).thenReturn(2L);
        when(resultSet.getString(2)).thenReturn("test_db1").thenReturn("test_db2");
        when(resultSet.getLong(3)).thenReturn(1L).thenReturn(2L);
        when(resultSet.getString(4)).thenReturn("test_tb1").thenReturn("test_tb2");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        List<HiveDbMetaDTO> hiveDbMetaList = hiveHelperService.fetchHiveDbMeta();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        String sql = (String) ReflectionTestUtils.getField(HiveHelperService.class, "DB_META_SQL");
        Assertions.assertThat(sqlCaptor.getValue()).isEqualTo(sql);

        Assertions.assertThat(hiveDbMetaList.size()).isEqualTo(2);
        Assertions.assertThat(hiveDbMetaList.get(0).getDbId()).isEqualTo(1L);
        Assertions.assertThat(hiveDbMetaList.get(0).getDb()).isEqualTo("test_db1");
        Assertions.assertThat(hiveDbMetaList.get(0).getTableId()).isEqualTo(1L);
        Assertions.assertThat(hiveDbMetaList.get(0).getTable()).isEqualTo("test_tb1");

        Assertions.assertThat(hiveDbMetaList.get(1).getDbId()).isEqualTo(2L);
        Assertions.assertThat(hiveDbMetaList.get(1).getDb()).isEqualTo("test_db2");
        Assertions.assertThat(hiveDbMetaList.get(1).getTableId()).isEqualTo(2L);
        Assertions.assertThat(hiveDbMetaList.get(1).getTable()).isEqualTo("test_tb2");

    }

    @Test
    public void testDescTable() throws SQLException {
        String sql = "SELECT c.COLUMN_NAME,c.TYPE_NAME FROM DBS d JOIN TBLS t ON d.DB_ID=t.DB_ID JOIN SDS s ON t.SD_ID=s.SD_ID JOIN COLUMNS_V2 c ON s.cd_id=c.cd_id WHERE d.NAME=? AND t.tbl_name=?";
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getString(1)).thenReturn("id").thenReturn("name");
        when(resultSet.getString(2)).thenReturn("int").thenReturn("string");
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        List<FieldDTO> fields = hiveHelperService.descTable("db_test", "tb_test");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        Assertions.assertThat(sqlCaptor.getValue()).isEqualTo(sql);

        ArgumentCaptor<String> databaseCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tableCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(statement).setString(eq(1), databaseCaptor.capture());
        Mockito.verify(statement).setString(eq(2), tableCaptor.capture());
        Assertions.assertThat(databaseCaptor.getValue()).isEqualTo("db_test");
        Assertions.assertThat(tableCaptor.getValue()).isEqualTo("tb_test");

        Assertions.assertThat(fields.size()).isEqualTo(2);
        Assertions.assertThat(fields.get(0).getName()).isEqualTo("id");
        Assertions.assertThat(fields.get(0).getDataType()).isEqualTo("int");

        Assertions.assertThat(fields.get(1).getName()).isEqualTo("name");
        Assertions.assertThat(fields.get(1).getDataType()).isEqualTo("string");
    }
}
