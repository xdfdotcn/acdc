package cn.xdf.acdc.devops.service.process.widetable.sql.transformer;

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

@Deprecated
public class SubQueryReplacementTransformerV1 implements SqlTransformer<Object> {
    
    private static final String EMPTY_STRING = "";
    
    @Override
    public SqlNode transform(final SqlNode sqlNode, final Object any) {
        recurse(sqlNode, null);
        return sqlNode;
    }
    
    @Override
    public String getName() {
        return this.getClass().getName();
    }
    
    /**
     * Recurse sql node.
     *
     * @param sqlNode sql node
     * @param parentSqlNode parent sql node
     */
    public void recurse(final SqlNode sqlNode, final SqlNode parentSqlNode) {
        // 递归边界
        if (isAsWithSingleTableSbuQuery(sqlNode)) {
            return;
        }
        if (isAsWithOriginalTable(sqlNode)) {
            replaceWithSubQuery(toSqlBasicCall(sqlNode), parentSqlNode);
            return;
        }
        
        if (isOriginalTable(sqlNode)) {
            replaceWithSubQuery(toSqlIdentifier(sqlNode), parentSqlNode);
            return;
        }
        
        // 递归处理
        if (isOrderBy(sqlNode)) {
            recurse(selectOfOrderByOf(sqlNode), sqlNode);
            return;
        }
        
        if (isAsWithSelect(sqlNode)) {
            recurse(asLeftOf(sqlNode), sqlNode);
            return;
        }
        
        if (isSelect(sqlNode)) {
            recurse(fromOf(sqlNode), sqlNode);
            return;
        }
        
        // 中序遍历 join node
        if (isJoin(sqlNode)) {
            recurse(joinRightOf(sqlNode), sqlNode);
            // do nothing
            recurse(joinLeftOf(sqlNode), sqlNode);
            return;
        }
        
        throw new AcdcServiceException("Node types in the scope are no longer processed " + sqlNode);
    }
    
    private void replaceWithSubQuery(final SqlBasicCall current, final SqlNode parent) {
        SqlNode subQuery = generateSubQuerySqlNode(
                current.getOperandList().get(0).toString(),
                current.getOperandList().get(1).toString()
        );
        
        updateJoinNodeChild(current, parent, subQuery);
    }
    
    private void replaceWithSubQuery(final SqlIdentifier current, final SqlNode parent) {
        SqlNode subQuery = generateSubQuerySqlNode(
                current.toString(),
                current.toString()
        );
        
        updateJoinNodeChild(current, parent, subQuery);
    }
    
    private SqlNode generateSubQuerySqlNode(final String table, final String tableAlias) {
        // select *
        SqlIdentifier asteriskIdentifier = new SqlIdentifier(
                Lists.newArrayList(EMPTY_STRING),
                SqlParserPos.ZERO
        );
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        sqlNodeList.add(asteriskIdentifier);
        
        // 原始表名：table
        SqlIdentifier fromIdentifier = new SqlIdentifier(
                Lists.newArrayList(table),
                SqlParserPos.ZERO
        );
        
        // as 操作
        SqlAsOperator fromSqlAsOperator = new SqlAsOperator();
        
        // 表别名: table
        SqlIdentifier fromAliasIdentifier = new SqlIdentifier(
                Lists.newArrayList(table),
                SqlParserPos.ZERO
        );
        
        // table as  table
        SqlBasicCall fromAsBasicCall = new SqlBasicCall(
                fromSqlAsOperator,
                Lists.newArrayList(fromIdentifier, fromAliasIdentifier),
                SqlParserPos.ZERO
        );
        
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
        
        // table as table
        SqlAsOperator subQuerySqlAsOperator = new SqlAsOperator();
        SqlIdentifier subQueryAliasIdentifier = new SqlIdentifier(
                Lists.newArrayList(tableAlias),
                SqlParserPos.ZERO
        );
        
        // (select * from table)  table
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
        if (!isJoin(parent)) {
            return;
        }
        
        SqlJoin parentJoin = toSqlJoin(parent);
        if (isJoinLeft(currentChild, parentJoin)) {
            parentJoin.setLeft(newChild);
        } else {
            parentJoin.setRight(newChild);
        }
    }
    
    // calcite
    private boolean isAs(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.AS;
    }
    
    private boolean isIdentifier(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.IDENTIFIER;
    }
    
    private boolean isSelect(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.SELECT;
    }
    
    private boolean isJoin(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.JOIN;
    }
    
    private boolean isOrderBy(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.ORDER_BY;
    }
    
    // custom
    private boolean isAsWithOriginalTable(final SqlNode sqlNode) {
        return isAs(sqlNode)
                && isIdentifier(asLeftOf(sqlNode))
                && isIdentifier(asRightOf(sqlNode));
    }
    
    private boolean isAsWithSingleTableSbuQuery(final SqlNode sqlNode) {
        if (!isAsWithSelect(sqlNode)) {
            return false;
        }
        
        SqlNode from = fromOf(asLeftOf(sqlNode));
        
        if (isSelect(from) || isAsWithSelect(from) || isJoin(from)) {
            return false;
        }
        return isIdentifier(from) || isIdentifier(asLeftOf(from));
    }
    
    private boolean isAsWithSelect(final SqlNode sqlNode) {
        return isAs(sqlNode)
                && isSelect(asLeftOf(sqlNode));
    }
    
    private boolean isOriginalTable(final SqlNode sqlNode) {
        return isIdentifier(sqlNode);
    }
    
    private boolean isJoinLeft(final SqlNode child, final SqlJoin parent) {
        return parent.getLeft() == child;
    }
    
    private SqlNode fromOf(final SqlNode sqlNode) {
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        return sqlSelect.getFrom();
    }
    
    private SqlNode asLeftOf(final SqlNode sqlNode) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        return sqlBasicCall.getOperandList().get(0);
    }
    
    private SqlNode asRightOf(final SqlNode sqlNode) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        return sqlBasicCall.getOperandList().get(1);
    }
    
    private SqlNode joinLeftOf(final SqlNode sqlNode) {
        SqlJoin sqlJoin = (SqlJoin) sqlNode;
        return sqlJoin.getLeft();
    }
    
    private SqlNode joinRightOf(final SqlNode sqlNode) {
        SqlJoin sqlJoin = (SqlJoin) sqlNode;
        return sqlJoin.getRight();
    }
    
    private SqlNode selectOfOrderByOf(final SqlNode sqlNode) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
        return sqlOrderBy.query;
    }
    
    private SqlBasicCall toSqlBasicCall(final SqlNode sqlNode) {
        return (SqlBasicCall) sqlNode;
    }
    
    private SqlJoin toSqlJoin(final SqlNode sqlNode) {
        return (SqlJoin) sqlNode;
    }
    
    private SqlIdentifier toSqlIdentifier(final SqlNode sqlNode) {
        return (SqlIdentifier) sqlNode;
    }
}
