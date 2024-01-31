package cn.xdf.acdc.devops.service.process.widetable.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

import com.google.common.collect.Lists;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;

public interface SqlSelectNodeRecursion {
    /**
     * 递归遍历 SqlNode.
     *
     * @param sqlNode SqlNode
     * @param <T> 返回结果
     * @param handler SqlNodeHandler
     * @return 执行结果
     */
    default <T> T recurse(final SqlNode sqlNode, final SqlNodeHandler<T> handler) {
        handler.onBefore(sqlNode);
        doRecurse(sqlNode, handler);
        return handler.onAfter(sqlNode);
    }
    
    /**
     * 执行递归遍历 SqlNode.
     *
     * @param <T> 返回结果
     * @param sqlNode SqlNode
     * @param handler SqlNodeHandler
     */
    default <T> void doRecurse(final SqlNode sqlNode, final SqlNodeHandler<T> handler) {
        // 递归边界
        if (isAsWithOrderByAtomQuery(sqlNode)) {
            handler.onVisitAtomQuery(
                    toSqlOrderBy(asLeftOf(sqlNode)),
                    asRightOf(sqlNode).toString()
            );
            
            return;
        }
        if (isOrderByAtomQuery(sqlNode)) {
            SqlOrderBy sqlOrderBy = toSqlOrderBy(sqlNode);
            SqlSelect sqlSelect = toSqlSelect(orderByQueryOf(sqlNode));
            handler.onVisitAtomQuery(sqlOrderBy, getAtomTableNameIgnoreSchema(sqlSelect.getFrom()));
            
            return;
        }
        if (isAsWithAtomQuery(sqlNode)) {
            handler.onVisitAtomQuery(toSqlSelect(asLeftOf(sqlNode)), asRightOf(sqlNode).toString());
            
            return;
        }
        if (isAtomQuery(sqlNode)) {
            handler.onVisitAtomQuery(toSqlSelect(sqlNode), getAtomTableNameIgnoreSchema(toSqlSelect(sqlNode).getFrom()));
            
            return;
        }
        
        // node 拆分传给下一层处理
        if (isAsWithSqlOrderBy(sqlNode)) {
            doRecurse(asLeftOf(sqlNode), handler);
            
            return;
        }
        if (isSqlOrderBy(sqlNode)) {
            doRecurse(orderByQueryOf(sqlNode), handler);
            
            return;
        }
        if (isAsWithSqlSelect(sqlNode)) {
            doRecurse(asLeftOf(sqlNode), handler);
            
            return;
        }
        
        // 处理关联查询
        if (isJoinedQuery(sqlNode)) {
            handler.onBeforeVisitJoinedQuery(toSqlSelect(sqlNode));
            
            doRecurse(fromOf(sqlNode), handler);
            
            handler.onAfterVisitJoinedQuery(toSqlSelect(sqlNode));
            
            return;
        }
        
        // 处理嵌套查询
        if (isNestedQuery(sqlNode)) {
            handler.onBeforeVisitNestedQuery(toSqlSelect(sqlNode));
            
            doRecurse(fromOf(sqlNode), handler);
            
            handler.onAfterVisitNestedQuery(toSqlSelect(sqlNode));
            
            return;
        }
        
        // join处理, join 的递归处理,递归顺序为前序遍历，1.遍历Join根节点，2.遍历右子树，3遍历左子树
        if (isSqlJoin(sqlNode)) {
            handler.onBeforeVisitJoin(toSqlJoin(sqlNode));
            
            if (!isAtomTable(joinRightOf(sqlNode))) {
                doRecurse(joinRightOf(sqlNode), handler);
            }
            if (!isAtomTable(joinLeftOf(sqlNode))) {
                doRecurse(joinLeftOf(sqlNode), handler);
            }
            
            handler.onAfterVisitJoin(toSqlJoin(sqlNode));
            
            return;
        }
        
        // 所有应该关注 case 都应该体现在以上流程判断里面，如果出现了 case 之外的情况，需停止处理
        throw new AcdcServiceException("Node types in the scope are no longer processed " + sqlNode);
    }
    
    /**
     * 是否为 'AS' 操作符.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAs(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.AS;
    }
    
    /**
     * 是否为 'OR' 操作符.
     *
     * @param condition SqlNode
     * @return true, false
     */
    static boolean isOr(final SqlNode condition) {
        return condition.getKind() == SqlKind.OR;
    }
    
    /**
     * 是否为 'AND' 操作符.
     *
     * @param condition SqlNode
     * @return true, false
     */
    static boolean isAnd(final SqlNode condition) {
        return condition.getKind() == SqlKind.AND;
    }
    
    /**
     * 是否为 SqlIdentifier.
     *
     * @param sqlNode sqlNode
     * @return true, false
     */
    static boolean isSqlIdentifier(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.IDENTIFIER;
    }
    
    /**
     * 是否为 SqlLiteral.
     *
     * @param sqlNode sqlNode
     * @return true, false
     */
    static boolean isSqlLiteral(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.LITERAL;
    }
    
    /**
     * 是否为 SqlSelect.
     *
     * @param sqlNode sqlNode
     * @return true, false
     */
    static boolean isSqlSelect(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.SELECT;
    }
    
    /**
     * 是否为 SqlJoin.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isSqlJoin(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.JOIN;
    }
    
    /**
     * 是否为 SqlOrderBy.
     *
     * @param sqlNode sqlNode
     * @return true, false
     */
    static boolean isSqlOrderBy(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.ORDER_BY;
    }
    
    /**
     * 判断是否为 SqlOrderBy 的 ‘AS’ 操作.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAsWithSqlOrderBy(final SqlNode sqlNode) {
        return isAs(sqlNode)
                && isSqlOrderBy(asLeftOf(sqlNode));
    }
    
    /**
     * 是否为 SqlBasicCall.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isSqlBasicCall(final SqlNode sqlNode) {
        return sqlNode instanceof SqlBasicCall;
    }
    
    /**
     * 是否为函数类型的查询列.
     *
     * @param sqlNodeListCol SqlNode
     * @return true, false
     */
    static boolean isFunctionSqlNodeListCol(final SqlNode sqlNodeListCol) {
        SqlNode col;
        if (isAs(sqlNodeListCol)) {
            col = asLeftOf(sqlNodeListCol);
        } else {
            col = sqlNodeListCol;
        }
        return isSqlBasicCall(col);
    }
    
    /**
     * 是否为原子查询的 ‘AS’ 操作.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAsWithAtomQuery(final SqlNode sqlNode) {
        if (!isAsWithSqlSelect(sqlNode)) {
            return false;
        }
        
        return isAtomQuery(asLeftOf(sqlNode));
    }
    
    /**
     * 是否为原子查询.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAtomQuery(final SqlNode sqlNode) {
        if (!isSqlSelect(sqlNode)) {
            return false;
        }
        
        SqlNode from = fromOf(sqlNode);
        
        if (isQuery(from)) {
            return false;
        }
        return isSqlIdentifier(from) || isSqlIdentifier(asLeftOf(from));
    }
    
    /**
     * 是否为查询.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isQuery(final SqlNode sqlNode) {
        return isSqlSelect(sqlNode)
                || isAsWithSqlSelect(sqlNode)
                || isSqlOrderBy(sqlNode)
                || isAsWithSqlOrderBy(sqlNode)
                || isSqlJoin(sqlNode);
    }
    
    /**
     * 是否为 SqlOrderBy 类型的原子查询的 ‘AS’ 操作.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAsWithOrderByAtomQuery(final SqlNode sqlNode) {
        return isAsWithSqlOrderBy(sqlNode)
                && isAtomQuery(orderByQueryOf(asLeftOf(sqlNode)));
    }
    
    /**
     * 是否为 orderBy 类型的原子查询.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isOrderByAtomQuery(final SqlNode sqlNode) {
        return isSqlOrderBy(sqlNode)
                && isAtomQuery(orderByQueryOf(sqlNode));
    }
    
    /**
     * 是否为关联查询.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isJoinedQuery(final SqlNode sqlNode) {
        return isSqlSelect(sqlNode)
                && isSqlJoin(fromOf(sqlNode));
    }
    
    /**
     * 是否为嵌套查询.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isNestedQuery(final SqlNode sqlNode) {
        return isSqlSelect(sqlNode)
                && isQuery(fromOf(sqlNode));
    }
    
    /**
     * 是否为原子表.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAtomTable(final SqlNode sqlNode) {
        if (isAs(sqlNode)) {
            return isSqlIdentifier(asLeftOf(sqlNode));
        }
        return isSqlIdentifier(sqlNode);
    }
    
    /**
     * 是否为 SqlSelect 的 ‘AS’ 操作.
     *
     * @param sqlNode SqlNode
     * @return true, false
     */
    static boolean isAsWithSqlSelect(final SqlNode sqlNode) {
        return isAs(sqlNode)
                && isSqlSelect(asLeftOf(sqlNode));
    }
    
    /**
     * 是否为子查询谓词.
     *
     * @param predicate SqlNode
     * @return true, false
     */
    static boolean isSubQueryPredicate(final SqlNode predicate) {
        SqlNode right = toSqlBasicCall(predicate).getOperandList().get(1);
        return isSqlOrderBy(right) || isSqlSelect(right);
    }
    
    /**
     * 获取 SqlSelect 的 From 属性.
     *
     * @param sqlNode SqlNode
     * @return SqlNode
     */
    static SqlNode fromOf(final SqlNode sqlNode) {
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        return sqlSelect.getFrom();
    }
    
    /**
     * 获取 'AS' 操作符的左侧内容.
     *
     * @param sqlNode SqlNode
     * @return SqlNode
     */
    static SqlNode asLeftOf(final SqlNode sqlNode) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        return sqlBasicCall.getOperandList().get(0);
    }
    
    /**
     * 获取 'AS' 操作符的右侧内容.
     *
     * @param sqlNode SqlNode
     * @return SqlNode
     */
    static SqlNode asRightOf(final SqlNode sqlNode) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        return sqlBasicCall.getOperandList().get(1);
    }
    
    /**
     * 获取 SqlJoin 的左侧内容.
     *
     * @param sqlNode SqlNode
     * @return SqlNode
     */
    static SqlNode joinLeftOf(final SqlNode sqlNode) {
        SqlJoin sqlJoin = (SqlJoin) sqlNode;
        return sqlJoin.getLeft();
    }
    
    /**
     * 获取 SqlJoin 的右侧内容.
     *
     * @param sqlNode SqlNode
     * @return SqlNode
     */
    static SqlNode joinRightOf(final SqlNode sqlNode) {
        SqlJoin sqlJoin = (SqlJoin) sqlNode;
        return sqlJoin.getRight();
    }
    
    /**
     * 获取条件的左侧部分内容，左侧是字段，eg: a.id.
     *
     * @param condition SqlNode
     * @return SqlNode
     */
    static SqlBasicCall conditionLeftOf(final SqlNode condition) {
        SqlBasicCall basicCall = (SqlBasicCall) condition;
        SqlBasicCall left = (SqlBasicCall) basicCall.getOperandList().get(0);
        return left;
    }
    
    /**
     * 获取条件右侧内容，右侧是字面量后者是唯一标识符(eg:a.id=1,a.id=b.id).
     *
     * @param condition SqlNode
     * @return SqlNode
     */
    static SqlBasicCall conditionRightOf(final SqlNode condition) {
        SqlBasicCall basicCall = (SqlBasicCall) condition;
        SqlBasicCall right = (SqlBasicCall) basicCall.getOperandList().get(1);
        return right;
    }
    
    /**
     * 如果为 SqlOrderBy 类型的 SqlNode, 获取其中的 query 属性.
     *
     * @param sqlNode SqlNode
     * @return SqlNode
     */
    static SqlNode orderByQueryOf(final SqlNode sqlNode) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
        return sqlOrderBy.query;
    }
    
    /**
     * 获取列的左侧值, eg: "db1.tb1.id",获取到的值为 "db1.tb1".
     *
     * @param column SqlNode
     * @return 列左侧的字符串
     */
    static String columnLeftStringOf(final SqlNode column) {
        // 字面两类型处理，eg： select 'abc' as code from table
        if (isSqlLiteral(column)) {
            return column.toString();
        }
        
        // 存在 database.table.column 的情况，数组长度可能是3或者2
        SqlIdentifier identifier = toSqlIdentifier(column);
        List<String> names = new ArrayList<>();
        for (int i = 0; i < identifier.names.size() - 1; i++) {
            names.add(identifier.names.get(i));
        }
        
        return SqlIdentifier.getString(names);
    }
    
    /**
     * 获取列的右侧值, eg: "a.id",获取到的值为 "id".
     *
     * @param column SqlNode
     * @return 列右侧的字符串
     */
    static String columnRightStringOf(final SqlNode column) {
        // 字面两类型处理，eg： select 'abc' as code from table
        if (isSqlLiteral(column)) {
            return column.toString();
        }
        
        // names 数组最后一个为为 column 名称,最后一位之前的都是table source alias
        SqlIdentifier identifier = toSqlIdentifier(column);
        return SqlIdentifier.getString(
                Lists.newArrayList(
                        identifier.names.get(identifier.names.size() - 1)
                )
        );
    }
    
    /**
     * 获取列的左侧, eg: "a.id=3",获取到的值为 "a.id".
     *
     * @param predicate SqlBasicCall
     * @return 谓词左侧的 SqlNode
     */
    static SqlNode predicateLeftOf(final SqlBasicCall predicate) {
        SqlBasicCall sqlBasicCall = predicate;
        SqlNode left = sqlBasicCall.getOperandList().get(0);
        return left;
    }
    
    /**
     * 获取列的右侧, eg: "a.id=3",获取到的值为 "3".
     *
     * @param predicate SqlBasicCall
     * @return 谓词左侧的 SqlNode
     */
    static SqlNode predicateRightOf(final SqlBasicCall predicate) {
        SqlBasicCall sqlBasicCall = predicate;
        SqlNode right = sqlBasicCall.getOperandList().get(1);
        return right;
    }
    
    /**
     * 获取原子查询的 table  source 名称.
     *
     * @param atomSbuQuery SqlSelect
     * @return table source 名称
     */
    static String getAtomQueryTableSourceNameIgnoreSchema(final SqlSelect atomSbuQuery) {
        SqlNode from = fromOf(atomSbuQuery);
        if (isAs(from)) {
            return getTableSourceNameIgnoreSchema(asLeftOf(from));
        }
        return getTableSourceNameIgnoreSchema(from);
    }
    
    /**
     * 获取 table source 的名称 忽略 schema信息.
     *
     * @param tableSource SqlNode
     * @return table source 名称
     */
    static String getTableSourceNameIgnoreSchema(final SqlNode tableSource) {
        SqlIdentifier identifier = toSqlIdentifier(tableSource);
        return identifier.names.get(identifier.names.size() - 1);
    }
    
    /**
     * 获取原子表名称,如果不存在.
     *
     * @param table table SqlNode
     * @return 原子表名称
     */
    static String getAtomTableNameIgnoreSchema(final SqlNode table) {
        if (isAs(table)) {
            return getTableSourceNameIgnoreSchema(asLeftOf(table));
        }
        return getTableSourceNameIgnoreSchema(table);
    }
    
    /**
     * 获取查询列的别名.
     *
     * @param sqlNodeListCol SqlNode
     * @return 查询列的别名
     */
    static String getSqlNodeListColAlias(final SqlNode sqlNodeListCol) {
        if (isAs(sqlNodeListCol)) {
            return asRightOf(sqlNodeListCol).toString();
        }
        
        return SystemConstant.EMPTY_STRING;
    }
    
    /**
     * 获取查询列的表达式 eg: 'a.id' 'SUBSTRING(a.id, 1)'.
     *
     * @param sqlNodeListCol SqlNode
     * @return 查询列的名称
     */
    static String getSqlNodeListColExpression(final SqlNode sqlNodeListCol) {
        if (isAs(sqlNodeListCol)) {
            return asLeftOf(sqlNodeListCol).toString();
        }
        
        return sqlNodeListCol.toString();
    }
    
    /**
     * 获取查询列的名称.
     *
     * @param sqlNodeListCol SqlNode
     * @return 查询列的名称
     */
    static String getSqlNodeListColName(final SqlNode sqlNodeListCol) {
        if (isAs(sqlNodeListCol)) {
            if (isFunctionSqlNodeListCol(sqlNodeListCol)) {
                return asLeftOf(sqlNodeListCol).toString();
            }
            return columnRightStringOf(asLeftOf(sqlNodeListCol)).toString();
        }
        if (isFunctionSqlNodeListCol(sqlNodeListCol)) {
            return sqlNodeListCol.toString();
        }
        
        return columnRightStringOf(sqlNodeListCol);
        
    }
    
    /**
     * 获取查询列的 table source 别名.
     *
     * @param sqlNodeListCol SqlNode
     * @return table source 别名
     */
    static String getSqlNodeListColTableSourceAlias(final SqlNode sqlNodeListCol) {
        if (isFunctionSqlNodeListCol(sqlNodeListCol)) {
            return SystemConstant.EMPTY_STRING;
        }
        
        if (isAs(sqlNodeListCol)) {
            return columnLeftStringOf(asLeftOf(sqlNodeListCol));
        }
        return columnLeftStringOf(sqlNodeListCol);
    }
    
    /**
     * 把函数查询列拆分成普通单列.
     *
     * @param sqlNodeListCol SqlNode
     * @return 单列结合
     */
    static List<SqlIdentifier> splitFunctionSqlNodeListCol(
            final SqlNode sqlNodeListCol
    ) {
        if (!isFunctionSqlNodeListCol(sqlNodeListCol)) {
            throw new AcdcServiceException("Current column is not function column");
        }
        
        SqlNode functionColumn;
        if (isAs(sqlNodeListCol)) {
            functionColumn = asLeftOf(sqlNodeListCol);
        } else {
            functionColumn = sqlNodeListCol;
        }
        
        List<SqlIdentifier> columns = new ArrayList<>();
        
        SqlBasicCall sqlBasicCall = toSqlBasicCall(functionColumn);
        List<SqlNode> oList = sqlBasicCall.getOperandList();
        
        for (SqlNode node : oList) {
            // 1. 如果SqlIdentifier 则获取对应的列值
            if (isSqlIdentifier(node)) {
                columns.add(toSqlIdentifier(node));
            }
            // 2. 如果是SqlBasicCall 则展开继续递归
            if (isSqlBasicCall(node)) {
                columns.addAll(splitFunctionSqlNodeListCol(node));
            }
            // 3. 函数中涉及的字面量不予处理
        }
        
        return columns;
    }
    
    /**
     * 谓词抽取，不支持 or 和 子查询谓词.
     *
     * @param condition SqlNode
     * @return 谓词集合
     */
    static List<SqlBasicCall> extractPredicate(
            final SqlNode condition
    ) {
        List<SqlBasicCall> predicates = new ArrayList<>();
        
        if (Objects.isNull(condition)) {
            return predicates;
        }
        
        if (isOr(condition)) {
            throw new AcdcServiceException("The 'OR' condition is not supported");
        }
        
        if (isAnd(condition)) {
            SqlBasicCall right = conditionRightOf(condition);
            SqlBasicCall left = conditionLeftOf(condition);
            predicates.addAll(extractPredicate(right));
            predicates.addAll(extractPredicate(left));
            return predicates;
        }
        
        // 递归边界
        if (isSubQueryPredicate(condition)) {
            throw new AcdcServiceException("The sub query condition is not supported");
        }
        predicates.add(toSqlBasicCall(condition));
        
        return predicates;
    }
    
    /**
     * 转换为 SqlBasicCall.
     *
     * @param sqlNode sqlNode
     * @return SqlNode
     */
    static SqlBasicCall toSqlBasicCall(final SqlNode sqlNode) {
        return (SqlBasicCall) sqlNode;
    }
    
    /**
     * 转换为 SqlSelect.
     *
     * @param sqlNode sqlNode
     * @return SqlNode
     */
    static SqlSelect toSqlSelect(final SqlNode sqlNode) {
        return (SqlSelect) sqlNode;
    }
    
    /**
     * 转换为 SqlJoin.
     *
     * @param sqlNode sqlNode
     * @return SqlNode
     */
    static SqlJoin toSqlJoin(final SqlNode sqlNode) {
        return (SqlJoin) sqlNode;
    }
    
    /**
     * 转换为 SqlIdentifier.
     *
     * @param sqlNode sqlNode
     * @return SqlNode
     */
    static SqlIdentifier toSqlIdentifier(final SqlNode sqlNode) {
        return (SqlIdentifier) sqlNode;
    }
    
    /**
     * 转换为 SqlOrderBy.
     *
     * @param sqlNode sqlNode
     * @return SqlNode
     */
    static SqlOrderBy toSqlOrderBy(final SqlNode sqlNode) {
        return (SqlOrderBy) sqlNode;
    }
    
    /**
     * 创建递归器.
     *
     * @return SqlSelectNodeRecursion
     */
    static SqlSelectNodeRecursion create() {
        return new SqlSelectNodeRecursion() {
        };
    }
    
    /**
     * 递归在遍历到对应的节点时候对调用对应的回调方法,所有子类自己实现对应的SqlNode处理逻辑.
     *
     * @param <T> 返回结果
     */
    interface SqlNodeHandler<T> {
        /**
         * 递归开始前调用.
         *
         * @param sqlNode SqlNode
         */
        default void onBefore(final SqlNode sqlNode) {
        }
        
        /**
         * 递归结束后调用.
         *
         * @param sqlNode SqlNode
         * @return 执行结果
         */
        default T onAfter(SqlNode sqlNode) {
            throw new UnsupportedOperationException();
        }
        
        /**
         * 递归前进阶段,访问 SqlJoin 时调用.
         *
         * @param sqlJoin SqlJoin
         */
        default void onBeforeVisitJoin(SqlJoin sqlJoin) {
        }
        
        /**
         * 递归回退阶段,访问 SqlJoin 时调用.
         *
         * @param sqlJoin SqlJoin
         */
        default void onAfterVisitJoin(SqlJoin sqlJoin) {
        }
        
        /**
         * 递归前进阶段,访问关联查询时调用,eg: select a.id,b.id from a left join b.
         *
         * @param sqlSelect SqlSelect
         */
        default void onBeforeVisitJoinedQuery(SqlSelect sqlSelect) {
        }
        
        /**
         * 递归回退阶段,访问关联查询时调用.
         *
         * @param sqlSelect SqlSelect
         */
        default void onAfterVisitJoinedQuery(SqlSelect sqlSelect) {
        }
        
        /**
         * 递归前进阶段,访问嵌套查询时调用，eg: select a.id from (select aa.id from aa) a.
         *
         * @param sqlSelect SqlSelect
         */
        default void onBeforeVisitNestedQuery(SqlSelect sqlSelect) {
        }
        
        /**
         * 递归回退阶段,访问嵌套查询时调用，eg: select a.id from (select * from a.id) a.
         *
         * @param sqlSelect SqlSelect
         */
        default void onAfterVisitNestedQuery(SqlSelect sqlSelect) {
        }
        
        /**
         * 递归前进阶段,访问原子查询时调用.
         *
         * @param sqlSelect SqlSelect
         * @param alias 原子查询别名，如果不存在则使用原子表名
         */
        default void onVisitAtomQuery(SqlSelect sqlSelect, String alias) {
        }
        
        /**
         * 递归前进阶段,访问原子查询(OrderBy子查询)时调用.
         *
         * @param sqlOrderBy SqlOrderBy
         * @param alias 原子查询别名，如果不存在则使用原子表名
         */
        default void onVisitAtomQuery(SqlOrderBy sqlOrderBy, String alias) {
        }
    }
}
