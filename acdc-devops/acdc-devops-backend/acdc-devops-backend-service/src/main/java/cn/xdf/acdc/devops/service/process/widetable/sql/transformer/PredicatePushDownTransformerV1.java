package cn.xdf.acdc.devops.service.process.widetable.sql.transformer;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.util.*;


@Deprecated
public class PredicatePushDownTransformerV1 implements SqlTransformer<Object> {
    
    public static final SqlParserPos POS = SqlParserPos.QUOTED_ZERO;
    
    private static final String CABLE = "-";
    
    private static final String ASTERISK = "*";
    
    private final SqlNodeProcessor sqlNodeProcessor = new SqlNodeProcessor();
    
    @Override
    public SqlNode transform(final SqlNode sqlNode, final Object any) {
        recurse(sqlNode);
        return sqlNode;
    }
    
    @Override
    public String getName() {
        return this.getClass().getName();
    }
    
    public void recurse(final SqlNode sqlNode) {
        // 递归边界
        if (isAsWithSingleTableSbuQuery(sqlNode)) {
            sqlNodeProcessor.onSingleTableSubQuery(toSqlSelect(asLeftOf(sqlNode)));
            return;
        }
        
        // 递归处理
        if (isOrderBy(sqlNode)) {
            recurse(selectOfOrderByOf(sqlNode));
            return;
        }
        
        if (isAsWithSelect(sqlNode)) {
            recurse(asLeftOf(sqlNode));
            return;
        }
        
        if (isSelect(sqlNode)) {
            sqlNodeProcessor.onSelect(toSqlSelect(sqlNode));
            recurse(fromOf(sqlNode));
            return;
        }
        
        // 前序遍历 join node
        if (isJoin(sqlNode)) {
            // root
            sqlNodeProcessor.onJoin(toSqlJoin(sqlNode));
            // right
            recurse(joinRightOf(sqlNode));
            // left
            recurse(joinLeftOf(sqlNode));
            return;
        }
        
        throw new AcdcServiceException("Node types in the scope are no longer processed " + sqlNode);
    }
    
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
    
    private SqlSelect toSqlSelect(final SqlNode sqlNode) {
        return (SqlSelect) sqlNode;
    }
    
    private SqlJoin toSqlJoin(final SqlNode sqlNode) {
        return (SqlJoin) sqlNode;
    }
    
    private boolean isLiteral(final SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.LITERAL;
    }
    
    private boolean isOr(final SqlNode condition) {
        return condition.getKind() == SqlKind.OR;
    }
    
    private boolean isAnd(final SqlNode condition) {
        return condition.getKind() == SqlKind.AND;
    }
    
    private boolean isSelectAllField(final SqlSelect sqlSelect) {
        SqlNodeList nodeList = sqlSelect.getSelectList();
        return CollectionUtils.isEmpty(nodeList)
                || (nodeList.size() == 1 && ASTERISK.equals(String.valueOf(nodeList.get(0))));
    }
    
    private boolean isSubQueryPredicate(final SqlBasicCall predicate) {
        SqlNode right = predicate.getOperandList().get(1);
        return isOrderBy(right) || isSelect(right);
    }
    
    private boolean isIdentifierPredicate(final SqlBasicCall predicate) {
        return isIdentifier(predicate.getOperandList().get(0))
                && isIdentifier(predicate.getOperandList().get(1));
    }
    
    private boolean isLiteralPredicate(final SqlBasicCall predicate) {
        return isIdentifier(predicate.getOperandList().get(0))
                && isLiteral(predicate.getOperandList().get(1));
    }
    
    private String predicateTableAliasOf(final SqlBasicCall predicate) {
        SqlNode left = predicateLeftOf(predicate);
        SqlNode right = predicateRightOf(predicate);
        
        if (isIdentifier(left) && isLiteral(right)) {               // 普通谓词
            return fieldLeftOf(toIdentifier(left));
        } else if (isIdentifier(left) && isIdentifier(right)) {     // 关联键谓词
            List<String> aliases = Lists.newArrayList(
                    fieldLeftOf(toIdentifier(left)),
                    fieldLeftOf(toIdentifier(right))
            );
            aliases.sort(Comparator.comparing(it -> it));
            
            return aliases.get(0) + CABLE + aliases.get(1);
            
        } else {                                                    // 不支持的谓词
            throw new AcdcServiceException("Unsupported predicate types" + predicate);
        }
    }
    
    private SqlNode predicateLeftOf(final SqlBasicCall predicate) {
        SqlBasicCall sqlBasicCall = predicate;
        SqlNode left = sqlBasicCall.getOperandList().get(0);
        return left;
    }
    
    private SqlNode predicateRightOf(final SqlBasicCall predicate) {
        SqlBasicCall sqlBasicCall = predicate;
        SqlNode right = sqlBasicCall.getOperandList().get(1);
        return right;
    }
    
    private String fieldLeftOf(final SqlIdentifier field) {
        if (field.names.size() < 2) {
            // TODO 这个需要根据字段从所有的源表中查询
            // where,on,select 中如果字段在结果集中唯一，可以不指定别名
            throw new AcdcServiceException("Predicates without table aliases are not supported");
        }
        String dotLeft = field.names.get(0);
        return dotLeft;
    }
    
    private String fieldRightOf(final SqlIdentifier field) {
        if (field.names.size() < 2) {
            throw new AcdcServiceException("Predicates without table aliases are not supported");
        }
        String dotRight = field.names.get(1);
        return dotRight;
    }
    
    private SqlBasicCall conditionLeftOf(final SqlNode condition) {
        SqlBasicCall basicCall = (SqlBasicCall) condition;
        SqlBasicCall left = (SqlBasicCall) basicCall.getOperandList().get(0);
        return left;
    }
    
    private SqlBasicCall conditionRightOf(final SqlNode condition) {
        SqlBasicCall basicCall = (SqlBasicCall) condition;
        SqlBasicCall right = (SqlBasicCall) basicCall.getOperandList().get(1);
        return right;
    }
    
    private SqlIdentifier toIdentifier(final SqlNode sqlNode) {
        return (SqlIdentifier) sqlNode;
    }
    
    private String joinTableAliasOf(final SqlJoin sqlJoin) {
        PredicateSelector selector = new PredicateSelector(PredicateType.IDENTIFIER, true);
        SqlBasicCall condition = toSqlBasicCall(sqlJoin.getCondition());
        recurseCondition(
                condition,
                selector
        );
        
        if (selector.matchedPredicateMap.isEmpty()) {
            throw new AcdcServiceException("Incorrect join condition, no association condition was found" + sqlJoin);
        }
        
        return String.valueOf(selector.matchedPredicateMap.keySet().toArray()[0]);
    }
    
    private SqlBasicCall newCondition(
            final List<SqlBasicCall> predicates
    ) {
        if (CollectionUtils.isEmpty(predicates)) {
            return null;
        }
        
        SqlBasicCall left = predicates.get(0);
        
        for (int i = 1; i < predicates.size(); i++) {
            left = newCondition(left, predicates.get(i), SqlKind.AND);
        }
        
        return toSqlBasicCall(left);
    }
    
    private SqlBasicCall newCondition(
            final SqlNode left,
            final SqlNode right,
            final SqlKind sqlKind) {
        List<SqlNode> operandList = Lists.newArrayList(left, right);
        SqlOperator operator = new SqlBinaryOperator(sqlKind.name(), sqlKind, 24, false, null, null, null);
        SqlBasicCall sqlBasicCall = new SqlBasicCall(operator, operandList, SqlParserPos.ZERO);
        return sqlBasicCall;
    }
    
    private PredicateSelector extractPredicateFromWhere(final SqlBasicCall condition) {
        PredicateSelector selector = new PredicateSelector(PredicateType.ALL, false);
        
        if (Objects.isNull(condition)) {
            return selector;
        }
        
        recurseCondition(condition, selector);
        
        return selector;
    }
    
    private PredicateSelector extractPredicateFromHaving(final SqlBasicCall condition) {
        return extractPredicateFromWhere(condition);
    }
    
    private PredicateSelector extractPredicateFromOn(final SqlBasicCall condition) {
        PredicateSelector selector = new PredicateSelector(PredicateType.LITERAL, false);
        recurseCondition(condition, selector);
        
        return selector;
    }
    
    private void recurseCondition(
            final SqlBasicCall curCondition,
            PredicateSelector selector
    ) {
        if (selector.onlyOne
                && selector.matchedPredicateMap.size() == 1
        ) {
            return;
        }
        
        // 校验
        if (isOr(curCondition)) {
            throw new AcdcServiceException("The 'OR' condition is not supported");
        }
        
        if (isAnd(curCondition)) {
            // 中序遍历
            SqlBasicCall right = conditionRightOf(curCondition);
            SqlBasicCall left = conditionLeftOf(curCondition);
            recurseCondition(right, selector);
            recurseCondition(left, selector);
            return;
        }
        
        // 递归边界
        if (isSubQueryPredicate(curCondition)) {
            throw new AcdcServiceException("The sub query condition is not supported");
        }
        switch (selector.selectedType) {
            case LITERAL:
                addPredicateToSelector(curCondition, selector, isLiteralPredicate(curCondition));
                break;
            case IDENTIFIER:
                addPredicateToSelector(curCondition, selector, isIdentifierPredicate(curCondition));
                break;
            case ALL:
                addPredicateToSelector(curCondition, selector, true);
                break;
            default:
                throw new AcdcServiceException("Unknown select type: " + selector.selectedType);
        }
    }
    
    private void addPredicateToSelector(
            final SqlBasicCall predicate,
            final PredicateSelector selector,
            final boolean isMatched
    ) {
        if (isMatched) {
            addPredicateTo(predicate, selector.matchedPredicateMap);
        } else {
            addPredicateTo(predicate, selector.unMatchedPredicateMap);
        }
    }
    
    private void addPredicateTo(
            final SqlBasicCall predicate,
            final Map<String, List<SqlBasicCall>> predicateMap
    ) {
        String alias = predicateTableAliasOf(predicate);
        
        List<SqlBasicCall> predicates = predicateMap
                .computeIfAbsent(alias, value -> Lists.newArrayList());
        predicates.add(predicate);
    }
    
    private class SqlNodeProcessor {
        
        private Stack<Map<String, List<SqlBasicCall>>> stack;
        
        public SqlNodeProcessor() {
            stack = new Stack<>();
            stack.push(new HashMap<>());
        }
        
        private List<SqlBasicCall> toList(final Map<String, List<SqlBasicCall>> predicateMap) {
            List<SqlBasicCall> predicates = Lists.newArrayList();
            for (List<SqlBasicCall> elements : predicateMap.values()) {
                predicates.addAll(elements);
            }
            return predicates;
        }
        
        private Map<String, List<SqlBasicCall>> toMap(final List<SqlBasicCall> predicates) {
            Map<String, List<SqlBasicCall>> receivingContainer = new HashMap<>();
            
            for (SqlBasicCall predicate : predicates) {
                addPredicateTo(predicate, receivingContainer);
            }
            
            return receivingContainer;
        }
        
        private Map<String, List<SqlBasicCall>> merge(
                final Map<String, List<SqlBasicCall>> predicateMap1,
                final Map<String, List<SqlBasicCall>> predicateMap2
        ) {
            Map<String, List<SqlBasicCall>> newPredicateMap = new HashMap<>();
            newPredicateMap.putAll(predicateMap1);
            
            for (Map.Entry<String, List<SqlBasicCall>> entry : predicateMap2.entrySet()) {
                for (SqlBasicCall predicate : entry.getValue()) {
                    addPredicateTo(predicate, newPredicateMap);
                }
            }
            return newPredicateMap;
        }
        
        private List<SqlBasicCall> pickAndRemoveMatchedPredicate(
                final String tableAlias,
                final Map<String, List<SqlBasicCall>> predicates
        ) {
            List<SqlBasicCall> ownPredicates = predicates.remove(tableAlias);
            
            return CollectionUtils.isEmpty(ownPredicates) ? new ArrayList<>() : ownPredicates;
        }
        
        private void convertOwnPredicate(final List<SqlBasicCall> predicates, final SqlSelect sqlSelect) {
            // 在执行完 calcite 基础校验之后不会出现这种情况，字段都会展开
            if (isSelectAllField(sqlSelect)) {
                return;
            }
            
            SqlNodeList sqlNodeList = sqlSelect.getSelectList();
            for (SqlNode node : sqlNodeList) {
                for (SqlBasicCall predicate : predicates) {
                    String toPushDownField = fieldRightOf(toIdentifier(predicateLeftOf(predicate)));
                    
                    String selectAliasField;
                    String selectFieldTableAlias;
                    String selectOriginalField;
                    if (isAs(node)) {
                        selectAliasField = asRightOf(node).toString();
                        selectFieldTableAlias = fieldLeftOf(toIdentifier(asLeftOf(node)));
                        selectOriginalField = fieldRightOf(toIdentifier(asLeftOf(node)));
                    } else {
                        selectAliasField = fieldRightOf(toIdentifier(node));
                        selectFieldTableAlias = fieldLeftOf(toIdentifier(node));
                        selectOriginalField = selectAliasField;
                    }
                    // 子查询字段匹配到下推的谓词，转换成自己表的谓词
                    if (toPushDownField.equalsIgnoreCase(selectAliasField)) {
                        SqlIdentifier identifier = newFieldIdentifier(selectFieldTableAlias, selectOriginalField);
                        predicate.setOperand(0, identifier);
                    }
                }
            }
        }
        
        private void onJoin(final SqlJoin sqlJoin) {
            // 匹配 on 条件中的字面量谓词,添加到上一次下推到join的谓词上下文对象中
            PredicateSelector selector = extractPredicateFromOn(toSqlBasicCall(sqlJoin.getCondition()));
            Map<String, List<SqlBasicCall>> preToBePushDownPredicateMap = merge(
                    stack.pop(), selector.matchedPredicateMap
            );
            
            // 对关联键谓词 下推到 on 条件中
            List<SqlBasicCall> toBePushDownToOnPredicateList = Lists.newArrayList();
            toBePushDownToOnPredicateList.addAll(
                    toList(selector.unMatchedPredicateMap)
            );
            toBePushDownToOnPredicateList.addAll(
                    pickAndRemoveMatchedPredicate(joinTableAliasOf(sqlJoin), preToBePushDownPredicateMap)
            );
            SqlNode newOnCondition = newCondition(toBePushDownToOnPredicateList);
            setValue(sqlJoin, "condition", newOnCondition);
            
            // 谓词分配，先分配给右子树，剩余的分配给左子树
            Map<String, List<SqlBasicCall>> rightChildToBePushDownPredicateMap = toMap(
                    pickAndRemoveMatchedPredicate(
                            asRightOf(sqlJoin.getRight()).toString(),
                            preToBePushDownPredicateMap));
            
            // 上一层推送给join的谓词+join中的谓词重新入栈，分配给左子树处理
            stack.push(preToBePushDownPredicateMap);
            // 分配给右子树的谓词入栈，等待递归边界出栈
            stack.push(rightChildToBePushDownPredicateMap);
        }
        
        private void onSelect(final SqlSelect sqlSelect) {
            List<SqlBasicCall> preTobePushDownPredicates = toList(stack.pop());
            PredicateSelector whereSelector = extractPredicateFromWhere(toSqlBasicCall(sqlSelect.getWhere()));
            PredicateSelector havingSelector = extractPredicateFromHaving(toSqlBasicCall(sqlSelect.getHaving()));
            Map<String, List<SqlBasicCall>> nextToBePushDownPredicates = merge(
                    whereSelector.matchedPredicateMap,
                    havingSelector.matchedPredicateMap
            );
            
            if (!CollectionUtils.isEmpty(preTobePushDownPredicates)) { // 存在推送给 join 子查询的谓词，递归首次调用不会满足条件
                convertOwnPredicate(preTobePushDownPredicates, sqlSelect);
                for (SqlBasicCall predicate : preTobePushDownPredicates) {
                    addPredicateTo(predicate, nextToBePushDownPredicates);
                }
            }
            
            SqlBasicCall newWhere = newCondition(toList(whereSelector.unMatchedPredicateMap));
            SqlBasicCall newHaving = newCondition(toList(havingSelector.unMatchedPredicateMap));
            sqlSelect.setWhere(newWhere);
            sqlSelect.setHaving(newHaving);
            
            stack.push(nextToBePushDownPredicates);
        }
        
        private void onSingleTableSubQuery(final SqlSelect sqlSelect) {
            List<SqlBasicCall> toBePushDownPredicates = toList(stack.pop());
            toBePushDownPredicates.addAll(
                    toList(extractPredicateFromWhere(toSqlBasicCall(sqlSelect.getWhere())).matchedPredicateMap)
            );
            
            convertOwnPredicate(toBePushDownPredicates, sqlSelect);
            
            SqlNode newCondition = newCondition(toBePushDownPredicates);
            sqlSelect.setWhere(newCondition);
        }
        
        private SqlIdentifier newFieldIdentifier(final String tableAlias, final String field) {
            SqlIdentifier identifier = new SqlIdentifier(Lists.newArrayList(tableAlias, field), POS);
            return identifier;
        }
        
        private void setValue(
                final Object object,
                final String fieldName,
                final Object value) {
            try {
                Class<?> clazz = object.getClass();
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(object, value);
            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                throw new AcdcServiceException(e);
            }
        }
    }
    
    private class PredicateSelector {
        
        private final Map<String, List<SqlBasicCall>> matchedPredicateMap;
        
        private final Map<String, List<SqlBasicCall>> unMatchedPredicateMap;
        
        private final PredicateType selectedType;
        
        private final boolean onlyOne;
        
        private PredicateSelector(
                final PredicateType selectedType,
                final boolean onlyOne) {
            this.selectedType = selectedType;
            this.onlyOne = onlyOne;
            this.matchedPredicateMap = new HashMap<>();
            this.unMatchedPredicateMap = new HashMap<>();
        }
    }
    
    private enum PredicateType {
        IDENTIFIER, LITERAL, ALL
    }
}
