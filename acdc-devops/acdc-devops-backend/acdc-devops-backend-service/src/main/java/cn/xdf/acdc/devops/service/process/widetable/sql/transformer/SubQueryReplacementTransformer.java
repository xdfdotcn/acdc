package cn.xdf.acdc.devops.service.process.widetable.sql.transformer;

import cn.xdf.acdc.devops.service.process.widetable.sql.SqlSelectNodeRecursion;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.stereotype.Service;

@Service
public class SubQueryReplacementTransformer implements SqlTransformer<Object> {
    
    private static final String EMPTY_STRING = "";
    
    @Override
    public SqlNode transform(final SqlNode sqlNode, final Object any) {
        return SqlSelectNodeRecursion.create().recurse(
                sqlNode,
                new Handler()
        );
    }
    
    @Override
    public String getName() {
        return this.getClass().getName();
    }
    
    private void replaceWithSubQuery(final SqlBasicCall current, final SqlNode parent) {
        SqlNode subQuery = generateSubQuerySqlNode(
                SqlSelectNodeRecursion.toSqlIdentifier(current.getOperandList().get(0)),
                current.getOperandList().get(1).toString()
        );
        
        updateJoinNodeChild(current, parent, subQuery);
    }
    
    private void replaceWithSubQuery(final SqlIdentifier current, final SqlNode parent) {
        SqlNode subQuery = generateSubQuerySqlNode(
                current,
                SqlSelectNodeRecursion.getTableSourceNameIgnoreSchema(current)
        );
        
        updateJoinNodeChild(current, parent, subQuery);
    }
    
    private SqlNode generateSubQuerySqlNode(final SqlIdentifier tableSource, final String subQueryAlias) {
        // select *
        SqlIdentifier asteriskIdentifier = new SqlIdentifier(
                Lists.newArrayList(EMPTY_STRING),
                SqlParserPos.ZERO
        );
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        sqlNodeList.add(asteriskIdentifier);
        
        // 原始表名：database.table
        SqlIdentifier fromIdentifier = new SqlIdentifier(
                Lists.newArrayList(tableSource.names),
                SqlParserPos.ZERO
        );
        
        // as 操作
        SqlAsOperator fromSqlAsOperator = new SqlAsOperator();
        
        // 子查询内部表别名 (from table as  table)
        String fromAliasIdentifierName = SqlSelectNodeRecursion.getTableSourceNameIgnoreSchema(tableSource);
        SqlIdentifier fromAliasIdentifier = new SqlIdentifier(
                Lists.newArrayList(fromAliasIdentifierName),
                SqlParserPos.ZERO
        );
        SqlBasicCall fromAsBasicCall = new SqlBasicCall(
                fromSqlAsOperator,
                Lists.newArrayList(fromIdentifier, fromAliasIdentifier),
                SqlParserPos.ZERO
        );
        // (select * from table as table )
        SqlSelect sqlSelect = new SqlSelect(
                SqlParserPos.ZERO,
                null,
                sqlNodeList,
                fromAsBasicCall,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        
        // (select * from table as table )  table
        SqlAsOperator subQuerySqlAsOperator = new SqlAsOperator();
        SqlIdentifier subQueryAliasIdentifier = new SqlIdentifier(
                Lists.newArrayList(subQueryAlias),
                SqlParserPos.ZERO
        );
        SqlBasicCall subQueryAsBasicCall = new SqlBasicCall(
                subQuerySqlAsOperator,
                Lists.newArrayList(sqlSelect, subQueryAliasIdentifier),
                SqlParserPos.ZERO
        );
        
        return subQueryAsBasicCall;
    }
    
    private void updateJoinNodeChild(
            final SqlNode currentChild,
            final SqlNode parent,
            final SqlNode newChild
    ) {
        if (!SqlSelectNodeRecursion.isSqlJoin(parent)) {
            return;
        }
        
        SqlJoin parentJoin = SqlSelectNodeRecursion.toSqlJoin(parent);
        if (isJoinLeft(currentChild, parentJoin)) {
            parentJoin.setLeft(newChild);
        } else {
            parentJoin.setRight(newChild);
        }
    }
    
    private boolean isJoinLeft(final SqlNode child, final SqlJoin parent) {
        return parent.getLeft() == child;
    }
    
    private class Handler implements SqlSelectNodeRecursion.SqlNodeHandler<SqlNode> {
        
        @Override
        public SqlNode onAfter(final SqlNode sqlNode) {
            return sqlNode;
        }
        
        @Override
        public void onBeforeVisitJoin(final SqlJoin sqlJoin) {
            SqlNode left = sqlJoin.getLeft();
            SqlNode right = sqlJoin.getRight();
            if (SqlSelectNodeRecursion.isAtomTable(left)) {
                if (SqlSelectNodeRecursion.isAs(left)) {
                    replaceWithSubQuery(SqlSelectNodeRecursion.toSqlBasicCall(left), sqlJoin);
                } else {
                    replaceWithSubQuery(SqlSelectNodeRecursion.toSqlIdentifier(left), sqlJoin);
                }
            }
            if (SqlSelectNodeRecursion.isAtomTable(right)) {
                if (SqlSelectNodeRecursion.isAs(right)) {
                    replaceWithSubQuery(SqlSelectNodeRecursion.toSqlBasicCall(right), sqlJoin);
                } else {
                    replaceWithSubQuery(SqlSelectNodeRecursion.toSqlIdentifier(right), sqlJoin);
                }
            }
        }
    }
}
