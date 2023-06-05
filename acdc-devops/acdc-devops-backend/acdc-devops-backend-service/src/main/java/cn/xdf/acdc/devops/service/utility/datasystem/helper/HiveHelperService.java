package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.service.config.HiveJdbcProperties;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Hive jdbc service.
 */
@Service
@Slf4j
public class HiveHelperService {
    
    private static final String SQL_SHOW_DATABASES = " SELECT d.NAME FROM DBS d";
    
    private static final String SQL_SHOW_TABLES = " SELECT t.TBL_NAME FROM DBS d JOIN TBLS t ON d.DB_ID=t.DB_ID and  d.NAME=?";
    
    private static final String SQL_DESC_TABLE =
            "SELECT c.COLUMN_NAME,c.TYPE_NAME FROM DBS d JOIN TBLS t ON d.DB_ID=t.DB_ID JOIN SDS s ON t.SD_ID=s.SD_ID JOIN COLUMNS_V2 c ON s.cd_id=c.cd_id WHERE d.NAME=? AND t.tbl_name=?";
    
    @Autowired
    private MysqlHelperService mysqlHelperService;
    
    @Autowired
    private HiveJdbcProperties config;
    
    /**
     * Get hive databases .
     *
     * @return databases
     */
    public List<String> showDatabases() {
        return mysqlHelperService.executeQuery(
                config.getUrl(),
                new UsernameAndPassword(config.getUser(), config.getPassword()),
                SQL_SHOW_DATABASES,
                this::doSingleStringColumnResultMapping);
    }
    
    /**
     * Get hive tables for specified database .
     *
     * @param database database
     * @return tables
     */
    public List<String> showTables(final String database) {
        return mysqlHelperService.executeQuery(
                config.getUrl(),
                new UsernameAndPassword(config.getUser(), config.getPassword()),
                SQL_SHOW_TABLES, stmt -> doShowTablesPreparedStatement(stmt, database), this::doSingleStringColumnResultMapping
        );
    }
    
    /**
     * Get hive table structure.
     *
     * @param database database
     * @param table table
     * @return table structure
     */
    public List<RelationalDatabaseTableField> descTable(final String database, final String table) {
        return mysqlHelperService.executeQuery(
                config.getUrl(),
                new UsernameAndPassword(config.getUser(), config.getPassword()),
                SQL_DESC_TABLE, stmt -> doDescTablePreparedStatement(stmt, database, table), this::doDescTableResultMapping
        );
    }
    
    private void doDescTablePreparedStatement(final PreparedStatement stmt, final String database, final String table) {
        try {
            stmt.setString(1, database);
            stmt.setString(2, table);
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        }
    }
    
    private List<RelationalDatabaseTableField> doDescTableResultMapping(final ResultSet rs) {
        List<RelationalDatabaseTableField> fieldList = Lists.newArrayList();
        try {
            while (rs.next()) {
                RelationalDatabaseTableField field = new RelationalDatabaseTableField()
                        .setName(rs.getString(1))
                        .setType(rs.getString(2));
                fieldList.add(field);
            }
            
            Preconditions.checkArgument(!CollectionUtils.isEmpty(fieldList), "Not exist field");
            return fieldList;
        } catch (SQLException e) {
            log.warn("Execute sql exception ", e);
            throw new ServerErrorException(e);
        }
    }
    
    private void doShowTablesPreparedStatement(final PreparedStatement stmt, final String database) {
        try {
            stmt.setString(1, database);
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        }
    }
    
    private List<String> doSingleStringColumnResultMapping(final ResultSet rs) {
        try {
            List<String> databases = Lists.newArrayList();
            while (rs.next()) {
                databases.add(rs.getString(1));
            }
            
            return databases;
        } catch (SQLException e) {
            log.warn("Execute sql exception ", e);
            throw new ServerErrorException(e);
        }
    }
}
