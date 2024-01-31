package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Properties;

public class MySqlSqlParser {
    
    /**
     * Parse sql to SqlNode.
     *
     * @param sql sql
     * @return SqlNode
     */
    public static SqlNode parse(final String sql) {
        SqlParser.Config config = getParserConfig();
        SqlParser parser = SqlParser.create(sql, config);
        
        try {
            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw new AcdcServiceException("Sql parsing encountered an error", e);
        }
    }
    
    private static CalciteConnectionConfig getCalciteConnectionConfig() {
        // 创建方言，配置相关
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL.toString());
        //properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        //properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        //properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(properties);
        
        return config;
    }
    
    private static SqlParser.Config getParserConfig() {
        CalciteConnectionConfig connectionConfig = getCalciteConnectionConfig();
        
        return SqlParser.config().withLex(connectionConfig.lex());
    }
}
