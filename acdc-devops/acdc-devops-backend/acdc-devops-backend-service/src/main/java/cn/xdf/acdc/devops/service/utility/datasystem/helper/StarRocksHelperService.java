package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.Mysql;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemConstant.Keyword;
import com.google.common.collect.Sets;
import io.jsonwebtoken.lang.Collections;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StarRocksHelperService {
    
    public static final String PRIMARY_KEY_ORDER = "0";
    
    public static final String ORDINARY_FIELD_ORDER = "1";
    
    public static final String INNER_FIELD_ORDER = "2";
    
    public static final String INNER_FIELD_PREFIX = "__";
    
    public static final String TABLE_MODEL = "TABLE_MODEL";
    
    public static final String TABLE_MODEL_PRIMARY_KEYS = "PRIMARY_KEYS";
    
    protected static final String PRIMARY_KEY = "PRIMARY_KEY";
    
    protected static final Set<String> UNIQUE_INDEX_NAMES = Sets.newHashSet(PRIMARY_KEY);
    
    private static final String QUERY_TABLE_MODEL_AND_PRIMARY_KEY_SQL = "select TABLE_MODEL, PRIMARY_KEY from information_schema.tables_config"
            + " where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'";
    
    private static final String QUERY_TABLE_FIELDS_SQL = "select COLUMN_NAME, COLUMN_TYPE from information_schema.columns"
            + " where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'";
    
    private static final String CHECK_HEALTH_SQL = "select 1";
    
    private static final String CREATE_TABLE_SQL = "create table if not exists %s (\n%s\n) PRIMARY KEY (%s) \nDISTRIBUTED BY HASH(%s)\nPROPERTIES (\n"
            + "\t\"enable_persistent_index\" = \"true\"\n)";
    
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
    
    /**
     * Create data collection if absent.
     *
     * @param hosts hosts
     * @param usernameAndPassword usernameAndPassword
     * @param databaseName databaseName
     * @param tableName tableName
     * @param fieldDefinitions fieldDefinitions
     * @param uniqueIndexNameToFieldDefinitions uniqueIndexNameToFieldDefinitions
     */
    public void createDataCollectionIfAbsent(final Set<HostAndPort> hosts,
                                             final UsernameAndPassword usernameAndPassword,
                                             final String databaseName,
                                             final String tableName,
                                             final List<DataFieldDefinition> fieldDefinitions,
                                             final Map<String, List<DataFieldDefinition>> uniqueIndexNameToFieldDefinitions) {
        HostAndPort hostAndPort = mysqlHelperService.choiceAvailableInstance(hosts, usernameAndPassword);
        
        // `database`.`table`
        String fullTableName = getFullTableName(databaseName, tableName);
        List<String> primaryKeyList = getPrimaryKey(uniqueIndexNameToFieldDefinitions);
        String fields = getFields(fieldDefinitions, primaryKeyList);
        String primaryKeyText = String.join(Symbol.COMMA, primaryKeyList);
        String sql = String.format(CREATE_TABLE_SQL, fullTableName, fields, primaryKeyText, primaryKeyText);
        log.info("The sql is: {}.", sql);
        
        mysqlHelperService.execute(hostAndPort, usernameAndPassword, sql);
    }
    
    private String getFields(final List<DataFieldDefinition> fieldDefinitions, final List<String> primaryKey) {
        List<String> fields = fieldDefinitions.stream()
                .sorted(Comparator.comparing(dataFieldDefinition -> generateSequence(dataFieldDefinition, primaryKey)))
                .map(dataFieldDefinition -> {
                    String fieldName = dataFieldDefinition.getName();
                    String fieldType = dataFieldDefinition.getType();
                    String nullDesc = primaryKey.contains(fieldName) ? Keyword.NOT_NULL : Keyword.NULL;
                    return Symbol.TAB + fieldName + Symbol.BLANK + fieldType + Symbol.BLANK + nullDesc;
                }).collect(Collectors.toList());
        return String.join(Symbol.COMMA + Symbol.ENTER, fields);
    }
    
    private String generateSequence(final DataFieldDefinition dataFieldDefinition, final List<String> primaryKey) {
        String fieldName = dataFieldDefinition.getName();
        String orderValue;
        int indexInPrimaryKey = primaryKey.indexOf(fieldName);
        if (indexInPrimaryKey != -1) {
            orderValue = PRIMARY_KEY_ORDER + indexInPrimaryKey + fieldName;
        } else if (fieldName.startsWith(INNER_FIELD_PREFIX)) {
            orderValue = INNER_FIELD_ORDER + fieldName;
        } else {
            orderValue = ORDINARY_FIELD_ORDER + fieldName;
        }
        return orderValue;
    }
    
    private List<String> getPrimaryKey(final Map<String, List<DataFieldDefinition>> uniqueIndexNameToFieldDefinitions) {
        List<DataFieldDefinition> fieldDefinitions = uniqueIndexNameToFieldDefinitions.get(Mysql.PK_INDEX_NAME);
        if (!Collections.isEmpty(fieldDefinitions)) {
            Set<DataFieldDefinition> nullableFields = fieldDefinitions.stream().filter(DataFieldDefinition::isOptional).collect(Collectors.toSet());
            if (Collections.isEmpty(nullableFields)) {
                return fieldDefinitions.stream().map(DataFieldDefinition::getName).collect(Collectors.toList());
            } else {
                throw new UnsupportedOperationException(String.format("Primary key field contain nullable fields: %s.", nullableFields));
            }
        }
        throw new UnsupportedOperationException("No primary key field is found.");
    }
    
    @NotNull
    private static String getFullTableName(final String databaseName, final String tableName) {
        return Symbol.BACK_TICK + databaseName + Symbol.BACK_TICK + Symbol.DOT + Symbol.BACK_TICK + tableName + Symbol.BACK_TICK;
    }
    
    @Getter
    @AllArgsConstructor
    static class TableModelAndPrimaryKey {
        
        private String tableModel;
        
        private Set<String> primaryKeyFieldNames;
    }
}
