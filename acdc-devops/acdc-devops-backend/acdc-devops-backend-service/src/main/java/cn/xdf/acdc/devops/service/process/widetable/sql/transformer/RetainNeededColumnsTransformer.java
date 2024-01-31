package cn.xdf.acdc.devops.service.process.widetable.sql.transformer;

import cn.xdf.acdc.devops.service.process.widetable.sql.TableField;
import cn.xdf.acdc.devops.service.process.widetable.sql.util.CalciteSqlNodeUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class RetainNeededColumnsTransformer implements SqlTransformer<Object> {
    
    public static final String RETAIN_NEEDED_COLUMNS_TRANSFORMER = "retainNeededColumns";
    
    public static final Set<SqlKind> SUPPORTED_CONDITION_IN_CLAUSE = Sets.immutableEnumSet(SqlKind.EQUALS, SqlKind.NOT_EQUALS,
            SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN, SqlKind.LESS_THAN, SqlKind.NOT_IN, SqlKind.IN);
    
    @Override
    public SqlNode transform(final SqlNode sqlNode, final Object any) {
        retainNeededColumns(sqlNode, new HashSet<>(), true);
        return sqlNode;
    }
    
    // 1. 获得上层传入字段，没有时以select list字段列表为初始字段
    // 2. 获取当前层where on having group by 和上面结果 放入新map传入下一层
    // 3. 下一层是单表，传入字段即为裁剪保留列
    // 4. 进入子node，map中获取当前node所属字段为上层传入待保留字段
    private void retainNeededColumns(final SqlNode sqlNode, final Set<TableField> upperNeeded, final boolean isOutermostLayer) {
        switch (sqlNode.getKind()) {
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                Set<TableField> retainFields = new HashSet<>();
                if (!upperNeeded.isEmpty()) {
                    sqlSelect.setSelectList(getNeededColumns(sqlSelect.getSelectList(), upperNeeded));
                } else if (!isOutermostLayer) {
                    sqlSelect.setSelectList(generatePlaceholderField());
                }
                retainFields.addAll(getOriginalFieldsFromSelectList(sqlSelect.getSelectList()));
                retainFields.addAll(getAllFieldsInClause(sqlSelect.getWhere()));
                retainFields.addAll(getAllFieldsInClause(sqlSelect.getHaving()));
                retainFields.addAll(CalciteSqlNodeUtil.getGroupByTableFields(sqlSelect.getGroup()));
                
                retainNeededColumns(Objects.requireNonNull(sqlSelect.getFrom()), retainFields, true);
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) sqlNode;
                Preconditions.checkState(SqlKind.AS == sqlJoin.getRight().getKind(), "Join's right node is a AS node.");
                SqlNode rightNodeAlias = ((SqlBasicCall) sqlJoin.getRight()).getOperandList().get(1);
                String rightNodeAliasName = ((SqlIdentifier) rightNodeAlias).names.get(0);
                Set<TableField> rightNodeNeededFields = upperNeeded.stream()
                        .filter(tableField -> tableField.getTable().equals(rightNodeAliasName)).collect(Collectors.toSet());
                Set<TableField> leftNodeNeededFields = upperNeeded.stream()
                        .filter(tableField -> !tableField.getTable().equals(rightNodeAliasName)).collect(Collectors.toSet());
                
                retainNeededColumns(sqlJoin.getLeft(), leftNodeNeededFields, true);
                retainNeededColumns(sqlJoin.getRight(), rightNodeNeededFields, true);
                break;
            case AS:
                SqlNode originalNode = ((SqlBasicCall) sqlNode).getOperandList().get(0);
                retainNeededColumns(originalNode, upperNeeded, true);
                break;
            default:
                break;
        }
    }
    
    private SqlNodeList generatePlaceholderField() {
        SqlNode sqlNode = SqlCharStringLiteral.createCharString("1", SqlParserPos.ZERO);
        List<SqlNode> sqlNodes = Lists.newArrayList(sqlNode);
        return SqlNodeList.of(SqlParserPos.ZERO, sqlNodes);
    }
    
    private SqlNodeList getNeededColumns(final SqlNodeList selectList, final Set<TableField> upperNeeded) {
        final Set<String> neededFieldSet = upperNeeded.stream().map(TableField::getField).collect(Collectors.toSet());
        List<SqlNode> sqlNodes = selectList.stream().filter(sqlNode -> {
            switch (sqlNode.getKind()) {
                case IDENTIFIER:
                    String field = ((SqlIdentifier) sqlNode).names.get(1);
                    return neededFieldSet.contains(field);
                case AS:
                    SqlIdentifier alias = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperandList().get(1);
                    String aliasName = alias.names.get(0);
                    return neededFieldSet.contains(aliasName);
                default:
                    throw new UnsupportedOperationException(String.format("Unsupported sql grammar: [select list not support: %s].", sqlNode));
            }
        }).collect(Collectors.toList());
        return SqlNodeList.of(SqlParserPos.ZERO, sqlNodes);
    }
    
    private Set<TableField> getAllFieldsInClause(final SqlNode sqlNode) {
        Set<TableField> result = new HashSet<>();
        getFieldsInClause(sqlNode, result);
        return result;
    }
    
    private void getFieldsInClause(final SqlNode sqlNode, final Set<TableField> result) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            // a.id = 10 / a.id=b.aid/a.id in (10, 11, 12)
            if (SUPPORTED_CONDITION_IN_CLAUSE.contains(sqlNode.getKind())) {
                sqlBasicCall.getOperandList().forEach(innerSqlNode -> {
                    dealWithIdentifier(result, innerSqlNode);
                });
            }
            if (SqlKind.AND == sqlNode.getKind() || SqlKind.OR == sqlNode.getKind()) {
                SqlNode left = sqlBasicCall.getOperandList().get(0);
                SqlNode right = sqlBasicCall.getOperandList().get(1);
                getFieldsInClause(left, result);
                getFieldsInClause(right, result);
            }
        }
    }
    
    private static void dealWithIdentifier(final Set<TableField> result, final SqlNode innerSqlNode) {
        if (SqlKind.IDENTIFIER == innerSqlNode.getKind()) {
            String table = ((SqlIdentifier) innerSqlNode).names.get(0);
            String field = ((SqlIdentifier) innerSqlNode).names.get(1);
            result.add(new TableField(table, field));
        }
    }
    
    private Set<TableField> getOriginalFieldsFromSelectList(final SqlNodeList selectList) {
        return selectList.stream().filter(sqlNode -> {
            if (SqlKind.AS == sqlNode.getKind()) {
                SqlNode originalNode = ((SqlBasicCall) sqlNode).getOperandList().get(0);
                return getIdentifiers(originalNode).size() > 0;
            }
            return true;
        }).flatMap(sqlNode -> {
            if (isSqlNodeCountFunction(sqlNode)) {
                return Stream.empty();
            }
            Set<SqlIdentifier> identifiers = getIdentifiers(sqlNode);
            return identifiers.stream().map(identifier -> {
                String table = identifier.names.get(0);
                String field = identifier.names.get(1);
                return new TableField(table, field);
            });
        }).collect(Collectors.toSet());
    }
    
    private boolean isSqlNodeCountFunction(final SqlNode sqlNode) {
        if (SqlKind.AS == sqlNode.getKind()) {
            SqlNode originalNode = ((SqlBasicCall) sqlNode).getOperandList().get(0);
            return isSqlNodeCountFunction(originalNode);
        }
        return SqlKind.COUNT == sqlNode.getKind();
    }
    
    private Set<SqlIdentifier> getIdentifiers(final SqlNode sqlNode) {
        if (SqlKind.IDENTIFIER == sqlNode.getKind()) {
            return Sets.newHashSet((SqlIdentifier) sqlNode);
        }
        if (SqlKind.AS == sqlNode.getKind()) {
            SqlNode originalNode = ((SqlBasicCall) sqlNode).getOperandList().get(0);
            return getIdentifiers(originalNode);
        }
        if (sqlNode instanceof SqlBasicCall) {
            return ((SqlBasicCall) sqlNode).getOperandList().stream()
                    .map(this::getIdentifiers)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
        }
        return Sets.newHashSet();
    }
    
    @Override
    public String getName() {
        return RETAIN_NEEDED_COLUMNS_TRANSFORMER;
    }
}
