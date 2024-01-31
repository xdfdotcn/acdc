package cn.xdf.acdc.devops.service.process.widetable.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.widetable.sql.transformer.SqlTransformer;
import cn.xdf.acdc.devops.service.process.widetable.sql.validator.SqlValidator;

@Service
public class WideTableSqlService {
    
    private static final String CAST_SIGNED_FORMAT = "CAST\\((.*?) AS SIGNED\\)";
    
    private static final String FIRST_MATCH_CHARS = "$1";
    
    private final SqlValidator<Map<String, Map<String, List<DataFieldDefinition>>>> calciteMysqlSqlValidator;
    
    private final SqlValidator<Map<SchemaTable, Set<Set<TableField>>>> customerSqlValidator;
    
    private final SqlTransformer retainNeededColumnsTransformer;
    
    private final SqlTransformer subQueryReplacementTransformer;
    
    private final SqlTransformer predicatePushDownTransformer;
    
    public WideTableSqlService(final @Qualifier("calciteMysqlSqlValidator") SqlValidator<Map<String, Map<String, List<DataFieldDefinition>>>> calciteMysqlSqlValidator,
                               final @Qualifier("customerSqlValidator") SqlValidator<Map<SchemaTable, Set<Set<TableField>>>> customerSqlValidator,
                               final @Qualifier("retainNeededColumnsTransformer") SqlTransformer retainNeededColumnsTransformer,
                               final @Qualifier("subQueryReplacementTransformer") SqlTransformer subQueryReplacementTransformer,
                               final @Qualifier("predicatePushDownTransformer") SqlTransformer predicatePushDownTransformer
    
    ) {
        this.calciteMysqlSqlValidator = calciteMysqlSqlValidator;
        this.customerSqlValidator = customerSqlValidator;
        this.retainNeededColumnsTransformer = retainNeededColumnsTransformer;
        this.subQueryReplacementTransformer = subQueryReplacementTransformer;
        this.predicatePushDownTransformer = predicatePushDownTransformer;
    }
    
    /**
     * Validate the sql with coordinating schemas and return sql node tree.
     *
     * @param schemaTableFieldsMap schema -> table -> fields
     * @param sql to parse sql
     * @return sqlNode tree
     * @throws SqlParseException sql parse exception
     */
    public SqlNode validate(final Map<String, Map<String, List<DataFieldDefinition>>> schemaTableFieldsMap, final String sql) throws SqlParseException {
        SqlNode sqlNode = SqlParser.create(sql, SqlParser.config().withLex(Lex.MYSQL)).parseStmt();
        sqlNode = calciteMysqlSqlValidator.validate(sqlNode, schemaTableFieldsMap);
        sqlNode = predicatePushDownTransformer.transform(sqlNode, schemaTableFieldsMap);
        sqlNode = subQueryReplacementTransformer.transform(sqlNode, null);
        // LISTAGG -> GROUP_CONCAT
        String mysqlSql = sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
        
        // 将关系代数转换的 `CAST(任意内容 AS SIGNED)` 的子串替换为 `任意内容`
        mysqlSql = removeCastSigned(mysqlSql);
        
        sqlNode = SqlParser.create(mysqlSql, SqlParser.config().withLex(Lex.MYSQL)).parseStmt();
        sqlNode = calciteMysqlSqlValidator.validate(sqlNode, schemaTableFieldsMap);
        sqlNode = customerSqlValidator.validate(sqlNode, getTableUks(schemaTableFieldsMap));
        sqlNode = retainNeededColumnsTransformer.transform(sqlNode, null);
        return sqlNode;
    }
    
    private String removeCastSigned(final String mysqlSql) {
        Pattern pattern = Pattern.compile(CAST_SIGNED_FORMAT);
        Matcher matcher = pattern.matcher(mysqlSql);
        return matcher.replaceAll(FIRST_MATCH_CHARS);
    }
    
    private Map<SchemaTable, Set<Set<TableField>>> getTableUks(final Map<String, Map<String, List<DataFieldDefinition>>> schemaTableFieldsMap) {
        Map<SchemaTable, Set<Set<TableField>>> tableUks = new HashMap<>();
        schemaTableFieldsMap.forEach((schema, tableFields) -> tableFields.forEach((table, fields) -> {
            Map<String, Set<TableField>> ukFields = new HashMap<>();
            fields.forEach(field -> {
                String fieldName = field.getName();
                field.getUniqueIndexNames().forEach(ukName -> ukFields.computeIfAbsent(ukName, key -> new HashSet<>()).add(new TableField(table, fieldName)));
            });
            
            tableUks.put(new SchemaTable(schema, table), new HashSet<>(ukFields.values()));
        }));
        return tableUks;
    }
}
