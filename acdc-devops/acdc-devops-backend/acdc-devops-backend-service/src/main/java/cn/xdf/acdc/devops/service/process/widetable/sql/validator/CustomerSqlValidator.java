package cn.xdf.acdc.devops.service.process.widetable.sql.validator;

import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.widetable.sql.FieldRelation;
import cn.xdf.acdc.devops.service.process.widetable.sql.SchemaTable;
import cn.xdf.acdc.devops.service.process.widetable.sql.TableField;
import cn.xdf.acdc.devops.service.process.widetable.sql.TableJoinRelationEnum;
import cn.xdf.acdc.devops.service.process.widetable.sql.TableRelation;
import cn.xdf.acdc.devops.service.process.widetable.sql.util.CalciteSqlNodeUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.jsonwebtoken.lang.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CustomerSqlValidator implements SqlValidator<Map<SchemaTable, Set<Set<TableField>>>> {
    
    public static final String CUSTOMER_SQL_VALIDATOR = "customer";
    
    public static final Set<JoinType> SUPPORTED_JOIN_TYPES = Sets.immutableEnumSet(JoinType.INNER, JoinType.LEFT, JoinType.RIGHT);
    
    public static final Set<String> SUPPORTED_FUNCTIONS_IN_SELECT_LIST = Sets.newHashSet("CAST", "CONCAT", "CONCAT_WS", "GROUP_CONCAT", "MIN", "MAX", "SUM", "COUNT");
    
    public static final Set<String> SUPPORTED_FUNCTIONS_IN_CONDITION = Sets.newHashSet("CONCAT", "CONCAT_WS", "CAST", "SUBSTRING");
    
    @Override
    public SqlNode validate(final SqlNode sqlNode, final Map<SchemaTable, Set<Set<TableField>>> tableUks) {
        acdcWideTableValidate(sqlNode, tableUks);
        return sqlNode;
    }
    
    private void acdcWideTableValidate(final SqlNode sqlNode, final Map<SchemaTable, Set<Set<TableField>>> tableUks) {
        // 初始传入语句最外层应为select
        Preconditions.checkArgument(SqlKind.SELECT == sqlNode.getKind(), "The sql should be a select sql.");
        checkSelectListWithSameFieldName(((SqlSelect) sqlNode).getSelectList());
        recurse(sqlNode, tableUks);
    }
    
    private void checkSelectListWithSameFieldName(final SqlNodeList selectList) {
        Set<String> fieldNames = new HashSet<>();
        selectList.forEach(sqlNode -> {
            switch (sqlNode.getKind()) {
                case AS:
                    assert sqlNode instanceof SqlBasicCall;
                    SqlBasicCall sqlAs = (SqlBasicCall) sqlNode;
                    SqlIdentifier alias = (SqlIdentifier) sqlAs.getOperandList().get(1);
                    String aliasName = alias.names.get(0);
                    if (!fieldNames.add(aliasName)) {
                        throwDuplicatedFieldNameException(aliasName);
                    }
                    break;
                case IDENTIFIER:
                    assert sqlNode instanceof SqlIdentifier;
                    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                    String identifierName = sqlIdentifier.names.get(1);
                    if (!fieldNames.add(identifierName)) {
                        throwDuplicatedFieldNameException(identifierName);
                    }
                    break;
                default:
                    // check in method: getTableFieldAndFillAsMappings
            }
        });
    }
    
    private void throwDuplicatedFieldNameException(final String aliasName) {
        throw new UnsupportedOperationException(String.format("Unsupported select list item name: [%s] which is duplicated.", aliasName));
    }
    
    private Set<Set<TableField>> recurse(final SqlNode sqlNode, final Map<SchemaTable, Set<Set<TableField>>> tableUks) {
        switch (sqlNode.getKind()) {
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                // 1. 验证where 接子查询
                if (ifScalarQueryOrSelectQueryInClause(sqlSelect.getWhere())) {
                    throw new UnsupportedOperationException("Unsupported sql grammar: [scalar query: SELECT in WHERE clause].");
                }
                
                // 2. 验证select list 中接子查询
                if (sqlSelect.getSelectList().stream().anyMatch(this::ifScalarQueryOrSelectQueryInClause)) {
                    throw new UnsupportedOperationException("Unsupported sql grammar: [scalar query: SELECT in SELECT list].");
                }
                
                // 3. 验证select list中函数是否支持
                throwExceptionIfFunctionInSelectListNotSupported(sqlSelect.getSelectList());
                
                // 4. 获取唯一键列表，如果有group by时需重新构建
                Set<Set<TableField>> candidateUks = recurse(Objects.requireNonNull(sqlSelect.getFrom()), tableUks);
                candidateUks = rebuildCandidateUksIfNeeded(candidateUks, sqlSelect.getGroup());
                
                // 5. 过滤select list中不存在的候选健及字段映射处理
                return getToPassUks(sqlSelect, candidateUks);
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) sqlNode;
                Set<Set<TableField>> leftUks = recurse(sqlJoin.getLeft(), tableUks);
                Set<Set<TableField>> rightUks = recurse(sqlJoin.getRight(), tableUks);
                
                // 1. join基础校验
                checkBasicItemsInSqlNode(sqlJoin);
                
                // 2. 关系校验 根据左右侧唯一键列表和on条件，进行判断是否满足关系，根据join结果和关系获得唯一键列表
                Map<TableRelation, Set<FieldRelation>> relations = getAllRelatedFieldsGroupByTable(sqlJoin.getCondition());
                // 右侧表始终时固定的表或子查询（非join）
                String rightTableName = getRightTableName(sqlJoin);
                TableJoinRelationEnum leftToRightJoinRelation = getLeftToRightJoinRelation(leftUks, rightUks, relations, rightTableName);
                
                return checkRelationsAndReturnToPassUks(sqlJoin, leftUks, rightUks, leftToRightJoinRelation);
            case AS:
                assert sqlNode instanceof SqlBasicCall;
                SqlBasicCall sqlAs = (SqlBasicCall) sqlNode;
                // index 0 before as, and 1 after as
                SqlNode innerNode = sqlAs.getOperandList().get(0);
                SqlIdentifier alias = (SqlIdentifier) sqlAs.getOperandList().get(1);
                Set<Set<TableField>> innerUks = recurse(innerNode, tableUks);
                return getUksWithNewTableName(innerUks, alias.getSimple());
            case IDENTIFIER:
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                // index 0 is always schema name, 1 is table name
                SchemaTable schemaTable = new SchemaTable(sqlIdentifier.names.get(0), sqlIdentifier.names.get(1));
                Set<Set<TableField>> passUks = tableUks.get(schemaTable);
                if (Collections.isEmpty(passUks)) {
                    throw new UnsupportedOperationException(String.format("No uk with table: %s.", schemaTable));
                }
                return passUks;
            default:
                throw new UnsupportedOperationException(String.format("Unknown sql node type: %s.", sqlNode.getKind()));
        }
    }
    
    private static Set<Set<TableField>> checkRelationsAndReturnToPassUks(final SqlJoin sqlJoin, final Set<Set<TableField>> leftUks,
                                                                         final Set<Set<TableField>> rightUks, final TableJoinRelationEnum leftToRightJoinRelation) {
        switch (sqlJoin.getJoinType()) {
            case LEFT:
                switch (leftToRightJoinRelation) {
                    case ONE_TO_ONE:
                    case MANY_TO_ONE:
                        return leftUks;
                    default:
                        throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [in left join, only support "
                                + "ONE_TO_ONE/MANY_TO_ONE], now is [%s], condition is [%s].", leftToRightJoinRelation, sqlJoin.getCondition()));
                }
            case RIGHT:
                switch (leftToRightJoinRelation) {
                    case ONE_TO_ONE:
                    case ONE_TO_MANY:
                        return rightUks;
                    default:
                        throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [in right join, only support "
                                + "ONE_TO_ONE/ONE_TO_MANY], now is [%s], condition is [%s].", leftToRightJoinRelation, sqlJoin.getCondition()));
                }
            case INNER:
                switch (leftToRightJoinRelation) {
                    case ONE_TO_ONE:
                        Set<Set<TableField>> mergedUks = new HashSet<>();
                        mergedUks.addAll(leftUks);
                        mergedUks.addAll(rightUks);
                        return mergedUks;
                    case ONE_TO_MANY:
                        return rightUks;
                    case MANY_TO_ONE:
                        return leftUks;
                    default:
                        throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [in inner join, only support "
                                + "ONE_TO_ONE/ONE_TO_MANY/MANY_TO_ONE], now is [%s], condition is [%s].", leftToRightJoinRelation, sqlJoin.getCondition()));
                }
            default:
                throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [join type: %s].", sqlJoin.getJoinType()));
        }
    }
    
    @NotNull
    private static Set<Set<TableField>> getToPassUks(final SqlSelect sqlSelect, final Set<Set<TableField>> candidateUks) {
        Map<TableField, TableField> asMappings = new HashMap<>();
        // 主键支持列表
        Set<TableField> presentTableFields = sqlSelect.getSelectList().stream()
                .filter(CustomerSqlValidator::filteredFunctionsBeforeAs)
                .map(selectItem -> getTableFieldAndFillAsMappings(asMappings, selectItem))
                .collect(Collectors.toSet());
        
        Set<Set<TableField>> passUks = candidateUks.stream()
                .filter(presentTableFields::containsAll)
                .map(uk -> uk.stream().map(field -> asMappings.getOrDefault(field, field)).collect(Collectors.toSet()))
                .collect(Collectors.toSet());
        
        if (Collections.isEmpty(passUks)) {
            log.info("select list:{} not include a unique key.", sqlSelect.getSelectList());
            throw new UnsupportedOperationException(String.format("No uk with select list: %s.", sqlSelect.getSelectList()));
        }
        return passUks;
    }
    
    @NotNull
    private static TableField getTableFieldAndFillAsMappings(final Map<TableField, TableField> asMappings, final SqlNode selectItem) {
        switch (selectItem.getKind()) {
            case AS:
                SqlBasicCall sqlBasicCall = (SqlBasicCall) selectItem;
                SqlIdentifier original = getIdentifier(sqlBasicCall.getOperandList().get(0));
                SqlIdentifier alias = (SqlIdentifier) sqlBasicCall.getOperandList().get(1);
                final TableField originalTableField = new TableField(original.names.get(0), original.names.get(1));
                final TableField aliasTableField = new TableField(null, alias.names.get(0));
                asMappings.put(originalTableField, aliasTableField);
                return originalTableField;
            case IDENTIFIER:
                assert selectItem instanceof SqlIdentifier;
                SqlIdentifier sqlIdentifier = (SqlIdentifier) selectItem;
                return new TableField(sqlIdentifier.names.get(0), sqlIdentifier.names.get(1));
            default:
                throw new UnsupportedOperationException(String.format("Unsupported select list item: [%s], only support [function/field] AS alias or field.", selectItem));
        }
    }
    
    private static SqlIdentifier getIdentifier(final SqlNode sqlNode) {
        if (SqlKind.IDENTIFIER == sqlNode.getKind()) {
            return (SqlIdentifier) sqlNode;
        }
        if (SqlKind.CAST == sqlNode.getKind()) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            return getIdentifier(sqlBasicCall.getOperandList().get(0));
        }
        throw new UnsupportedOperationException("Could not find sql identifier in: " + sqlNode);
    }
    
    private static boolean filteredFunctionsBeforeAs(final SqlNode selectItem) {
        if (SqlKind.AS == selectItem.getKind()) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectItem;
            SqlNode original = sqlBasicCall.getOperandList().get(0);
            // 主键只在 IDENTIFIER 中选，暂不考虑udf函数
            return SqlKind.IDENTIFIER == original.getKind() || isCastAndIdentifierOnly(original);
        } else {
            return SqlKind.IDENTIFIER == selectItem.getKind();
        }
    }
    
    private static boolean isCastAndIdentifierOnly(final SqlNode sqlNode) {
        if (SqlKind.CAST == sqlNode.getKind()) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlNode before = sqlBasicCall.getOperandList().get(0);
            return SqlKind.IDENTIFIER == before.getKind() || isCastAndIdentifierOnly(before);
        }
        return false;
    }
    
    private static Set<Set<TableField>> rebuildCandidateUksIfNeeded(final Set<Set<TableField>> candidateUks, final SqlNodeList groupByFields) {
        if (!Collections.isEmpty(groupByFields)) {
            Set<TableField> groupByTableFields = CalciteSqlNodeUtil.getGroupByTableFields(groupByFields);
            Set<Set<TableField>> newCandidateUks = new HashSet<>();
            newCandidateUks.add(groupByTableFields);
            return newCandidateUks;
        }
        return candidateUks;
    }
    
    private void checkBasicItemsInSqlNode(final SqlJoin sqlJoin) {
        // 1. 当前只支持inner, left, right join; 当from后接多表时，也会转化为joinNode，此时类型为：COMMA，也不支持
        if (!SUPPORTED_JOIN_TYPES.contains(sqlJoin.getJoinType())) {
            throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [join type: %s].", sqlJoin.getJoinType()));
        }
        // 2. on条件是否存在
        if (JoinConditionType.ON != sqlJoin.getConditionType()) {
            throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [join condition type: %s].", sqlJoin.getConditionType()));
        }
        // 3. on 条件存在子查询
        if (ifScalarQueryOrSelectQueryInClause(sqlJoin.getCondition())) {
            throw new UnsupportedOperationException("Unsupported sql grammar: [scalar query: SELECT in ON clause].");
        }
        // 4. on 条件存在or
        if (ifOrInClause(sqlJoin.getCondition())) {
            throw new UnsupportedOperationException("Unsupported sql grammar: [keyword: OR in ON condition].");
        }
        
        // 5. on 条件存在函数
        if (ifUnsupportedFunctionInClause(sqlJoin.getCondition())) {
            throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [unsupported function in ON condition %s,"
                    + " now supported function list: %s].", sqlJoin.getCondition(), SUPPORTED_FUNCTIONS_IN_CONDITION));
        }
    }
    
    // Preorder traversal, once or has been found, return true.
    private boolean ifUnsupportedFunctionInClause(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator() instanceof SqlFunction
                    && !SUPPORTED_FUNCTIONS_IN_CONDITION.contains(sqlBasicCall.getOperator().getName().toUpperCase())) {
                return true;
            }
            List<SqlNode> operandList = sqlBasicCall.getOperandList();
            if (operandList != null) {
                return operandList.stream().anyMatch(this::ifUnsupportedFunctionInClause);
            }
        }
        return false;
    }
    
    private void throwExceptionIfFunctionInSelectListNotSupported(final SqlNodeList selectList) {
        selectList.forEach(this::recurseSqlNodeThrowExceptionWithUnsupportedFunction);
    }
    
    private void recurseSqlNodeThrowExceptionWithUnsupportedFunction(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            final SqlOperator operator = sqlBasicCall.getOperator();
            if (operator instanceof SqlFunction && !SUPPORTED_FUNCTIONS_IN_SELECT_LIST.contains(operator.getName().toUpperCase())) {
                throw new UnsupportedOperationException(
                        String.format("Unsupported sql grammar: [unknown function: %s, now only support: %s].", operator, SUPPORTED_FUNCTIONS_IN_SELECT_LIST)
                );
            }
            sqlBasicCall.getOperandList().forEach(this::recurseSqlNodeThrowExceptionWithUnsupportedFunction);
        }
    }
    
    // Preorder traversal, once or has been found, return true.
    private boolean ifScalarQueryOrSelectQueryInClause(final SqlNode sqlNode) {
        if (sqlNode == null) {
            return false;
        }
        if (SqlKind.SCALAR_QUERY == sqlNode.getKind() || SqlKind.SELECT == sqlNode.getKind()) {
            return true;
        }
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperandList().size() == 2) {
                SqlNode left = sqlBasicCall.getOperandList().get(0);
                SqlNode right = sqlBasicCall.getOperandList().get(1);
                return ifScalarQueryOrSelectQueryInClause(left) || ifScalarQueryOrSelectQueryInClause(right);
            }
        }
        return false;
    }
    
    // Preorder traversal, once or has been found, return true.
    private boolean ifOrInClause(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (SqlKind.OR == sqlNode.getKind()) {
                return true;
            }
            // when operand list size is 2, the node is a condition, like AND, OR, <, > ,etc.
            if (sqlBasicCall.getOperandList().size() == 2) {
                SqlNode left = sqlBasicCall.getOperandList().get(0);
                SqlNode right = sqlBasicCall.getOperandList().get(1);
                return ifOrInClause(left) || ifOrInClause(right);
            }
        }
        return false;
    }
    
    private TableJoinRelationEnum getLeftToRightJoinRelation(final Set<Set<TableField>> leftUks, final Set<Set<TableField>> rightUks,
                                                             final Map<TableRelation, Set<FieldRelation>> relations, final String rightTableName) {
        boolean isRightOne = false;
        boolean isLeftOne = false;
        for (Map.Entry<TableRelation, Set<FieldRelation>> relation : relations.entrySet()) {
            TableRelation tableRelation = relation.getKey();
            Set<FieldRelation> fieldRelations = relation.getValue();
            if (Objects.equals(rightTableName, tableRelation.getFirst())) {
                isRightOne = isRightOne || rightUks.stream().anyMatch(getFirstInRelations(fieldRelations)::containsAll);
                isLeftOne = isLeftOne || leftUks.stream().anyMatch(getSecondInRelations(fieldRelations)::containsAll);
            }
            if (Objects.equals(rightTableName, tableRelation.getSecond())) {
                isRightOne = isRightOne || rightUks.stream().anyMatch(getSecondInRelations(fieldRelations)::containsAll);
                isLeftOne = isLeftOne || leftUks.stream().anyMatch(getFirstInRelations(fieldRelations)::containsAll);
            }
        }
        
        if (isLeftOne) {
            if (isRightOne) {
                return TableJoinRelationEnum.ONE_TO_ONE;
            } else {
                return TableJoinRelationEnum.ONE_TO_MANY;
            }
        } else {
            if (isRightOne) {
                return TableJoinRelationEnum.MANY_TO_ONE;
            } else {
                return TableJoinRelationEnum.MANY_TO_MANY;
            }
        }
    }
    
    @NotNull
    private static Set<TableField> getSecondInRelations(final Set<FieldRelation> fieldRelations) {
        return fieldRelations.stream().map(FieldRelation::getSecond).collect(Collectors.toSet());
    }
    
    @NotNull
    private static Set<TableField> getFirstInRelations(final Set<FieldRelation> fieldRelations) {
        return fieldRelations.stream().map(FieldRelation::getFirst).collect(Collectors.toSet());
    }
    
    private String getRightTableName(final SqlJoin sqlJoin) {
        SqlNode right = sqlJoin.getRight();
        if (SqlKind.AS == right.getKind()) {
            SqlNode sqlNode = ((SqlBasicCall) right).getOperandList().get(1);
            return ((SqlIdentifier) sqlNode).names.get(0);
        }
        throw new ServerErrorException("SqlJoin's right should be an AS sql basic call.");
    }
    
    private Map<TableRelation, Set<FieldRelation>> getAllRelatedFieldsGroupByTable(final SqlNode condition) {
        Map<TableRelation, Set<FieldRelation>> result = new HashMap<>();
        getRelatedFieldsGroupByTable(condition, result);
        return result;
    }
    
    private void getRelatedFieldsGroupByTable(final SqlNode sqlNode, final Map<TableRelation, Set<FieldRelation>> result) {
        if (SqlKind.EQUALS == sqlNode.getKind()) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlNode left = sqlBasicCall.getOperandList().get(0);
            SqlNode right = sqlBasicCall.getOperandList().get(1);
            left = isCastAndIdentifierOnly(left) ? getIdentifier(left) : left;
            right = isCastAndIdentifierOnly(right) ? getIdentifier(right) : right;
            if (SqlKind.IDENTIFIER == left.getKind() && SqlKind.IDENTIFIER == right.getKind()) {
                String leftTable = ((SqlIdentifier) left).names.get(0);
                String leftField = ((SqlIdentifier) left).names.get(1);
                String rightTable = ((SqlIdentifier) right).names.get(0);
                String rightField = ((SqlIdentifier) right).names.get(1);
                throwExceptionIfTheSameTableBeforeAndAfterOn(leftTable, rightTable);
                TableRelation tableRelation = TableRelation.getInstance(leftTable, rightTable);
                FieldRelation fieldRelation = FieldRelation.getInstance(new TableField(leftTable, leftField), new TableField(rightTable, rightField));
                result.computeIfAbsent(tableRelation, k -> new HashSet<>()).add(fieldRelation);
            }
        }
        if (SqlKind.AND == sqlNode.getKind()) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlNode left = sqlBasicCall.getOperandList().get(0);
            SqlNode right = sqlBasicCall.getOperandList().get(1);
            getRelatedFieldsGroupByTable(left, result);
            getRelatedFieldsGroupByTable(right, result);
        }
    }
    
    private void throwExceptionIfTheSameTableBeforeAndAfterOn(final String leftTable, final String rightTable) {
        if (Objects.equals(leftTable, rightTable)) {
            throw new UnsupportedOperationException("Unsupported sql grammar: [after ON ='s left and right couldn't be the same table].");
        }
    }
    
    private Set<Set<TableField>> getUksWithNewTableName(final Set<Set<TableField>> innerUks, final String tableName) {
        return innerUks.stream().map(
                tableFields -> tableFields.stream()
                        .map(tableField -> new TableField(tableName, tableField.getField()))
                        .collect(Collectors.toSet())
        ).collect(Collectors.toSet());
    }
    
    @Override
    public String getName() {
        return CUSTOMER_SQL_VALIDATOR;
    }
}
