package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.HiveDbMetaDTO;
import cn.xdf.acdc.devops.service.config.HiveJdbcConfig;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Hive jdbc service.
 */
@Service
@Slf4j
public class HiveHelperService extends AbstractMysqlHelperService {

    private static final String DB_META_SQL = "SELECT d.DB_ID,d.NAME,t.TBL_ID,t.TBL_NAME FROM DBS d JOIN TBLS t ON d.DB_ID=t.DB_ID";

    private static final String COLUMN_META_SQL =
        "SELECT c.COLUMN_NAME,c.TYPE_NAME FROM DBS d JOIN TBLS t ON d.DB_ID=t.DB_ID JOIN SDS s ON t.SD_ID=s.SD_ID JOIN COLUMNS_V2 c ON s.cd_id=c.cd_id WHERE d.NAME=? AND t.tbl_name=?";

    @Autowired
    private HiveJdbcConfig config;

    /**
     * 拉取 hive 元数据,库表信息.
     * @return List
     */
    public List<HiveDbMetaDTO> fetchHiveDbMeta() {
        return executeQuery(getConnection(), DB_META_SQL, this::mappingDbMetaSql);
    }

    /**
     * 查询表结构.
     * @param database  database
     * @param table  table
     * @return List
     */
    public List<FieldDTO> descTable(final String database, final String table) {
        return executeQuery(
            getConnection(),
            COLUMN_META_SQL,
            stmt -> setColumnMetaSql(stmt, database, table),
            this::mappingColumnMetaSql
        );
    }

    private List<HiveDbMetaDTO> mappingDbMetaSql(final ResultSet rs) {
        try {
            List<HiveDbMetaDTO> dbMetaList = Lists.newArrayList();
            while (rs.next()) {
                HiveDbMetaDTO meta = HiveDbMetaDTO.builder()
                    .dbId(rs.getLong(1))
                    .db(rs.getString(2))
                    .tableId(rs.getLong(3))
                    .table(rs.getString(4))
                    .build();
                dbMetaList.add(meta);
            }

            Preconditions.checkArgument(!CollectionUtils.isEmpty(dbMetaList), "Not exist database meta data");
            return dbMetaList;
        } catch (SQLException e) {
            log.warn("Execute sql exception ", e);
            throw new ServerErrorException(e);
        }
    }

    private void setColumnMetaSql(final PreparedStatement stmt, final String database, final String table) {
        try {
            stmt.setString(1, database);
            stmt.setString(2, table);
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        }
    }

    private List<FieldDTO> mappingColumnMetaSql(final ResultSet rs) {
        List<FieldDTO> fieldList = Lists.newArrayList();
        try {
            while (rs.next()) {
                FieldDTO field = FieldDTO.builder()
                    .name(rs.getString(1))
                    .dataType(rs.getString(2)).build();
                fieldList.add(field);
            }

            Preconditions.checkArgument(!CollectionUtils.isEmpty(fieldList), "Not exist field");
            return fieldList;
        } catch (SQLException e) {
            log.warn("Execute sql exception ", e);
            throw new ServerErrorException(e);
        }
    }

    private Connection getConnection() {
        return createConnection(config.getUrl(), config.getUser(), config.getPassword());
    }
}
