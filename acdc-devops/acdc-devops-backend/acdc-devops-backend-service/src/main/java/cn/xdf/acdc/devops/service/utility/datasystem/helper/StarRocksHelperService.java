package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;

@Service
@Slf4j
public class StarRocksHelperService {
    
    public static final String TABLE_MODEL = "TABLE_MODEL";
    
    public static final String TABLE_MODEL_PRIMARY_KEYS = "PRIMARY_KEYS";
    
    protected static final String PRIMARY_KEY = "PRIMARY_KEY";
    
    protected static final Set<String> UNIQUE_INDEX_NAMES = Sets.newHashSet(PRIMARY_KEY);
    
    private static final String QUERY_TABLE_MODEL_AND_PRIMARY_KEY_SQL = "select TABLE_MODEL, PRIMARY_KEY from information_schema.tables_config"
            + " where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'";
    
    private static final String QUERY_TABLE_FIELDS_SQL = "select COLUMN_NAME, COLUMN_TYPE from information_schema.columns"
            + " where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'";
    
    private static final String CHECK_HEALTH_SQL = "select 1";
    
    @Autowired
    private MysqlHelperService mysqlHelperService;
    
    /**
     * Get database names of an StarRocks instances.
     *
     * @param hostAndPorts host and port model
     * @param usernameAndPassword username and password model
     * @param predicate filter function
     * @return database list
     */
    public List<String> showDataBases(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword, final Predicate<String> predicate) {
        return mysqlHelperService.showDataBases(hostAndPorts, usernameAndPassword, predicate);
    }
    
    /**
     * Get table names of a StarRocks database.
     *
     * @param hostAndPorts host and port model
     * @param database database name
     * @param usernameAndPassword username and password model
     * @return table list
     */
    public List<String> showTables(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword, final String database) {
        return mysqlHelperService.showTables(hostAndPorts, usernameAndPassword, database);
    }
    
    /**
     * Get table info for a StarRocks table.
     *
     * @param hostAndPorts host and port model
     * @param usernameAndPassword username and password model
     * @param database database name
     * @param table table name
     * @return table filed list
     */
    public RelationalDatabaseTable descTable(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword, final String database, final String table) {
        HostAndPort hostAndPort = mysqlHelperService.choiceAvailableInstance(hostAndPorts, usernameAndPassword);
        
        RelationalDatabaseTable relationalDatabaseTable = new RelationalDatabaseTable();
        relationalDatabaseTable.setName(table);
        
        TableModelAndPrimaryKey tableModelAndPrimaryKey = queryTableModelAndPrimaryKey(hostAndPort, usernameAndPassword, database, table);
        
        // set properties for table
        Properties properties = new Properties();
        properties.put(TABLE_MODEL, tableModelAndPrimaryKey.getTableModel());
        relationalDatabaseTable.setProperties(properties);
        
        // set fields
        relationalDatabaseTable.setFields(queryTableFieldsAndSetUniqueIndexName(hostAndPort, usernameAndPassword, database, table, tableModelAndPrimaryKey.primaryKeyFieldNames));
        
        return relationalDatabaseTable;
    }
    
    protected TableModelAndPrimaryKey queryTableModelAndPrimaryKey(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword, final String database, final String table) {
        return mysqlHelperService.executeQuery(hostAndPort, usernameAndPassword, generateQueryTableModelAndPrimaryKeySQL(database, table), rs -> {
            try {
                if (rs.next()) {
                    String tableName = rs.getString(TABLE_MODEL);
                    String primaryKey = rs.getString(PRIMARY_KEY);
                    Set<String> primaryKeyFieldNames = convertPrimaryKeyToFieldNames(primaryKey);
                    
                    return new TableModelAndPrimaryKey(tableName, primaryKeyFieldNames);
                }
                throw new ServerErrorException(String.format("can not find table model and primary key info for table %s.%s@%s:%d", database, table, hostAndPort.getHost(), hostAndPort.getPort()));
            } catch (SQLException e) {
                throw new ServerErrorException("error when query table model and primary key", e);
            }
        });
    }
    
    protected String generateQueryTableModelAndPrimaryKeySQL(final String database, final String table) {
        return String.format(QUERY_TABLE_MODEL_AND_PRIMARY_KEY_SQL, database, table);
    }
    
    /**
     * Convert primary key string to field names.
     *
     * <p>
     * eg: from "`column_1`, `column_2`" to ["column_1", "column_2"]
     * </p>
     *
     * @param primaryKey primary key string in information_schema.tables_config.PRIMARY_KEY of StarRocks.
     * @return field names
     */
    private Set<String> convertPrimaryKeyToFieldNames(final String primaryKey) {
        if (StringUtils.isBlank(primaryKey)) {
            return Sets.newHashSet();
        }
        String[] primaryKeyFieldNames = primaryKey.split(CommonConstant.COMMA);
        
        for (int i = 0; i < primaryKeyFieldNames.length; i++) {
            // remove space in front of char
            primaryKeyFieldNames[i] = primaryKeyFieldNames[i].trim();
            // remove "`" around field name
            primaryKeyFieldNames[i] = primaryKeyFieldNames[i].substring(1, primaryKeyFieldNames[i].length() - 1);
        }
        
        return Sets.newHashSet(primaryKeyFieldNames);
    }
    
    protected List<RelationalDatabaseTableField> queryTableFieldsAndSetUniqueIndexName(
            final HostAndPort hostAndPort,
            final UsernameAndPassword usernameAndPassword,
            final String database,
            final String table,
            final Set<String> primaryKeyFieldNames) {
        List<RelationalDatabaseTableField> fields = queryTableFields(hostAndPort, usernameAndPassword, database, table);
        
        fields.forEach(each -> {
            if (primaryKeyFieldNames.contains(each.getName())) {
                each.setUniqueIndexNames(UNIQUE_INDEX_NAMES);
            }
        });
        
        return fields;
    }
    
    protected List<RelationalDatabaseTableField> queryTableFields(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword, final String database, final String table) {
        return mysqlHelperService.executeQuery(hostAndPort, usernameAndPassword, generateQueryTableFieldsSQL(database, table), rs -> {
            try {
                List<RelationalDatabaseTableField> fields = new ArrayList<>();
                
                while (rs.next()) {
                    RelationalDatabaseTableField databaseTableField = new RelationalDatabaseTableField();
                    databaseTableField.setName(rs.getString(1));
                    databaseTableField.setType(rs.getString(2));
                    fields.add(databaseTableField);
                }
                
                return fields;
            } catch (SQLException e) {
                throw new ServerErrorException("error when query table fields", e);
            }
        });
    }
    
    protected String generateQueryTableFieldsSQL(final String database, final String table) {
        return String.format(QUERY_TABLE_FIELDS_SQL, database, table);
    }
    
    /**
     * Check StarRocks cluster's health.
     *
     * @param hostAndPorts instance's hosts and ports
     * @param usernameAndPassword username and password
     */
    public void checkHealth(final Set<HostAndPort> hostAndPorts, final UsernameAndPassword usernameAndPassword) {
        HostAndPort hostAndPort = mysqlHelperService.choiceAvailableInstance(hostAndPorts, usernameAndPassword);
        mysqlHelperService.executeQuery(hostAndPort, usernameAndPassword, CHECK_HEALTH_SQL, rs -> {
            try {
                return rs.next();
            } catch (SQLException e) {
                throw new ServerErrorException(String.format("error when execute check health SQL on StarRocks FE node %s:%d", hostAndPort.getHost(), hostAndPort.getPort()), e);
            }
        });
    }
    
    /**
     * Check StarRocks permissions.
     *
     * @param hostAndPort instance host and port
     * @param usernameAndPassword username and password
     * @param requiredPermissions required permissions
     */
    public void checkPermissions(final HostAndPort hostAndPort, final UsernameAndPassword usernameAndPassword, final List<String[]> requiredPermissions) {
        throw new UnsupportedOperationException();
    }
    
    @Getter
    @AllArgsConstructor
    static class TableModelAndPrimaryKey {
        
        private String tableModel;
        
        private Set<String> primaryKeyFieldNames;
    }
}
