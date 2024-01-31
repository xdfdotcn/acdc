// CHECKSTYLE:OFF
package cn.xdf.acdc.devops.service.process.widetable.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnLineageDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnUniqueIndexDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryJoinConditionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSqlJoinType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSubqueryTableSourceType;
import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.widetable.IdGenerator;
import cn.xdf.acdc.devops.service.process.widetable.sql.SqlSelectNodeRecursion;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

import static cn.xdf.acdc.devops.service.process.widetable.sql.SqlSelectNodeRecursion.*;

public class SubQueryPersistenceSqlNodeHandler implements SqlSelectNodeRecursion.SqlNodeHandler<WideTableSubqueryDO> {
    private static final String DEFAULT_RESULT_NAME = "result";
    
    public static final String DEFAULT_SCHEMA_NAME = "default";
    
    public static final String ASTERISK = "*";
    
    private Function<ColumnKey, Object> aliasOrNameColKeyMapper = (colKey) -> Strings
            .isNullOrEmpty(colKey.alias) ? colKey.name : colKey.alias;
    
    private Function<ColumnKey, Object> nameColKeyMapper = (colKey) -> colKey.name;
    
    private Function<ColumnKey, Object> defaultColKeyMapper = (colKey) -> colKey;
    
    // 保存当前需要处理的SqlSelect，只能在一个关联查询，或者嵌套查询中产生，并且等待后面的所有子节点处理完成才能出栈
    private Stack<SqlSelect> sqlSelectStack;
    
    // 保存当前需要处理字段类型的列
    private Stack<Map<ColumnKey, List<TraceColumn>>> traceColumnStack;
    
    // 保存当前需要处理血缘关系的列
    private Stack<Map<ColumnKey, List<SubQueryColumn>>> lineageColumnStack;
    
    // 保存当前需要处理的 SqlNodeList column
    private Stack<Map<ColumnKey, SqlNode>> sqlNodeListColumnStack;
    
    // 保存当前需要处理的子查询对象
    private Stack<WideTableSubqueryDO> subQueryStack;
    
    // 最终需要存储的子查询对象
    private WideTableSubqueryDO root;
    
    // id 生成器
    private final IdGenerator idGenerator;
    
    // 数据集 字段声明
    private final Map<String, Map<String, DataFieldDefinition>> dataFieldDefinitionTable;
    
    // 数据集
    private final Map<String, DataSystemResourceDTO> dataSystemResourceMap;
    
    // 国际化组件
    private final I18nService i18n;
    
    public SubQueryPersistenceSqlNodeHandler(
            final IdGenerator idGenerator,
            final Map<String, Map<String, DataFieldDefinition>> dataFieldDefinitionTable,
            final Map<String, DataSystemResourceDTO> dataSystemResourceMap,
            final I18nService i18n
    ) {
        this.idGenerator = idGenerator;
        this.dataFieldDefinitionTable = dataFieldDefinitionTable;
        this.dataSystemResourceMap = dataSystemResourceMap;
        this.i18n = i18n;
    }
    
    @Override
    public void onBefore(SqlNode sqlNode) {
        this.sqlSelectStack = new Stack<>();
        this.traceColumnStack = new Stack<>();
        this.lineageColumnStack = new Stack<>();
        this.sqlNodeListColumnStack = new Stack<>();
        this.subQueryStack = new Stack<>();
        this.sqlNodeListColumnStack.push(new HashMap<>());
        this.traceColumnStack.push(new HashMap<>());
        this.lineageColumnStack.push(new HashMap<>());
        this.root = new WideTableSubqueryDO(idGenerator.id());
        this.subQueryStack.push(root);
    }
    
    @Override
    public WideTableSubqueryDO onAfter(SqlNode sqlNode) {
        return root;
    }
    
    /**
     * select a.id,b.id,c.id,c.name,a.name,b.name,concat(a.desc,b.desc,a.id) as desc from a left join b left join c
     * <p>
     * <p>
     * 1. 子查询产生的列
     * a.id,b.id,a.name,b.name,c.id,c.name,concat(a.desc,b.desc,a.id)
     * <p>
     * 2. 血缘关系产生的列
     * <p>
     * a.id,b.id,a.name,b.name,c.id,c.name,a.desc(concat对象),b.desc(concat对象),a.id(concat 对象)
     * <p>
     * 3. 产生的类型列
     * <p>
     * a.id,b.id,a.name,b.name,c.id,c.name,a.desc(concat对象),b.desc(concat对象),a.id(concat 对象)
     * <p>
     * 4. 产生的sqlNode 列
     * <p>
     * a.id,a.name,b.id,b.name,c.id,c.name,a.desc,b.desc(a.id 重复,拆分出来多的字段需要去除)
     * <p>
     * 5. 第1次join 左侧应该产生的列(a,b)
     * <p>
     * a.id,a.name,b.id,b.name,a.desc,b.desc c 部分移除掉了，只剩下a,b部分
     * <p>
     * 6. 第1次join 右侧产生(c)
     * <p>
     * id,name
     * <p>
     * 7. 第二次 join 左侧应该产生的列(a)
     * <p>
     * id,name,desc
     * <p>
     * 8. 第二次 join 右侧应该产生的列(b)
     * <p>
     * id,name,desc
     */
    @Override
    public void onBeforeVisitJoinedQuery(final SqlSelect sqlSelect) {
        WideTableSubqueryDO popUpSubQuery = handleQuery(sqlSelect);
        subQueryStack.push(popUpSubQuery);
    }
    
    @Override
    public void onAfterVisitJoinedQuery(final SqlSelect sqlSelect) {
        sqlSelectStack.pop();
    }
    
    @Override
    public void onBeforeVisitNestedQuery(final SqlSelect sqlSelect) {
        WideTableSubqueryDO popUpSubQuery = handleQuery(sqlSelect);
        WideTableSubqueryDO subQuery = new WideTableSubqueryDO(idGenerator.id());
        popUpSubQuery.setTableSourceType(WideTableSubqueryTableSourceType.SUBQUERY);
        popUpSubQuery.setSubquery(subQuery);
        subQueryStack.push(subQuery);
    }
    
    @Override
    public void onAfterVisitNestedQuery(final SqlSelect sqlSelect) {
        sqlSelectStack.pop();
    }
    
    @Override
    public void onBeforeVisitJoin(final SqlJoin sqlJoin) {
        handleJoin(sqlJoin);
    }
    
    @Override
    public void onAfterVisitJoin(final SqlJoin sqlJoin) {
        // do nothing
    }
    
    @Override
    public void onVisitAtomQuery(final SqlSelect sqlSelect, final String alias) {
        handleAtomQuery(sqlSelect, alias, sqlSelect.toString());
    }
    
    @Override
    public void onVisitAtomQuery(final SqlOrderBy sqlOrderBy, final String alias) {
        handleAtomQuery(toSqlSelect(orderByQueryOf(sqlOrderBy)), alias, sqlOrderBy.toString());
    }
    
    private WideTableSubqueryDO handleQuery(final SqlSelect sqlSelect) {
        Map<ColumnKey, List<SubQueryColumn>> popUpLineageColMapping = lineageColumnStack.pop();
        Map<ColumnKey, List<SubQueryColumn>> lineageColMapping = new HashMap<>();
        
        Map<ColumnKey, List<TraceColumn>> popUpTraceColMapping = traceColumnStack.pop();
        Map<ColumnKey, List<TraceColumn>> traceColMapping = new HashMap<>();
        
        sqlNodeListColumnStack.pop();
        
        Set<WideTableSubqueryColumnDO> tobePersistedCols = new HashSet<>();
        Set<SubQueryColumn> subQueryCols = new HashSet<>();
        Map<ColumnKey, SqlNode> sqlNodeListColMapping = new HashMap<>();
        
        // 1. 提取本层产生的查询列,本层待匹配血缘关系的列，本层函数列拆分,创建跟踪列
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        
        for (SqlNode node : sqlNodeList) {
            SubQueryColumn subQueryCol = new SubQueryColumn(new WideTableSubqueryColumnDO(), node);
            TraceColumn traceColumn = isFunctionSqlNodeListCol(node) ?
                    newTraceCol(
                            subQueryCol,
                            aliasOrNameColKeyMapper,
                            popUpTraceColMapping,
                            nameColKeyMapper,
                            splitFunctionSqlNodeListCol(subQueryCol.sqlNodeListCol).size())
                    :
                    newTraceCol(
                            subQueryCol,
                            aliasOrNameColKeyMapper,
                            popUpTraceColMapping,
                            nameColKeyMapper,
                            TraceColumn.SEMAPHORE_1);
            
            ColumnKey columnKey = subQueryCol.getColumnKey();
            setColBasic(subQueryCol);
            
            // 处理血缘列
            putToList(lineageColMapping, columnKey, subQueryCol);
            
            // 处理跟踪列
            putToList(
                    traceColMapping,
                    columnKey,
                    traceColumn
            );
            
            // 处理原始 sqlNode 查询列
            putAndCheckIfExist(sqlNodeListColMapping, columnKey, node);
            
            if (isFunctionSqlNodeListCol(node)) {
                splitColumn(subQueryCol, traceColumn, sqlNodeListColMapping, lineageColMapping, traceColMapping);
            }
            
            tobePersistedCols.add(subQueryCol.columnDO);
            subQueryCols.add(subQueryCol);
        }
        
        // 2. 创建子查询对象
        WideTableSubqueryDO popUpSubQuery = subQueryStack.pop();
        setSubQueryBasic(DEFAULT_RESULT_NAME, sqlSelect, popUpSubQuery);
        setColumnsCascade(popUpSubQuery, tobePersistedCols);
        setColLineage(subQueryCols, aliasOrNameColKeyMapper, popUpLineageColMapping, nameColKeyMapper);
        popUpSubQuery.setSelectStatement(sqlSelect.toString());
        popUpSubQuery.setReal(Boolean.TRUE);
        
        lineageColumnStack.push(lineageColMapping);
        traceColumnStack.push(traceColMapping);
        sqlNodeListColumnStack.push(sqlNodeListColMapping);
        sqlSelectStack.push(sqlSelect);
        return popUpSubQuery;
    }
    
    private void handleJoin(final SqlJoin sqlJoin) {
        // 需要处理的列进行分配
        String rightTableSourceAlias = asRightOf(sqlJoin.getRight()).toString();
        
        Pair<Map<ColumnKey, SqlNode>, Map<ColumnKey, SqlNode>> sqlNodeListColPair = binarySplit(
                rightTableSourceAlias,
                sqlNodeListColumnStack.pop()
        );
        
        Pair<Map<ColumnKey, List<SubQueryColumn>>, Map<ColumnKey, List<SubQueryColumn>>> lineageColPair = binarySplit(
                rightTableSourceAlias,
                lineageColumnStack.pop()
        );
        
        Pair<Map<ColumnKey, List<TraceColumn>>, Map<ColumnKey, List<TraceColumn>>> traceColPair = binarySplit(
                rightTableSourceAlias,
                traceColumnStack.pop()
        );
        
        // 分配给左子树处理的 SqlNodeList column
        Map<ColumnKey, SqlNode> leftSqlNodeListColMapping = sqlNodeListColPair.getLeft();
        Map<ColumnKey, SqlNode> rightSqlNodeListColMapping = sqlNodeListColPair.getRight();
        
        // 分配给右子树处理的 LineageColumn
        Map<ColumnKey, List<SubQueryColumn>> rightLineageColMapping = lineageColPair.getRight();
        // 分配给左子树处理的 LineageColumn
        Map<ColumnKey, List<SubQueryColumn>> leftLineageColMapping = lineageColPair.getLeft();
        
        // 分配给右子树处理的 ChildColumn
        Map<ColumnKey, List<TraceColumn>> rightTraceColMapping = traceColPair.getRight();
        // 分配给左子树处理的 ChildColumn
        Map<ColumnKey, List<TraceColumn>> leftTraceColMapping = traceColPair.getLeft();
        
        // 获取当前需要处理的SubQuery
        WideTableSubqueryDO popUpSubQuery = subQueryStack.pop();
        
        // 注意：只有在一个sqlSelect 的from全部处理完成子可以出栈
        SqlSelect currentSqlSelect = sqlSelectStack.peek();
        SqlNodeList originalSqlNodeList = currentSqlSelect.getSelectList();
        SqlNode originalWhere = currentSqlSelect.getWhere();
        SqlNode originalHaving = currentSqlSelect.getHaving();
        
        WideTableSubqueryDO leftSubQuery = new WideTableSubqueryDO(idGenerator.id());
        WideTableSubqueryDO rightSubQuery = new WideTableSubqueryDO(idGenerator.id());
        // 叶子节点可能出现在 join 的右侧，或者已经拆分到只有两个表的 join 如果是叶子节点无需在包装已成子查询
        if (isSqlJoin(sqlJoin.getLeft())) {
            // sql node 结构变形，进行左右拆分
            SqlJoin newSqlJoin = toSqlJoin(sqlJoin.getLeft());
            SqlNodeList newSqlNodeList = new SqlNodeList(ImmutableList.of(), SqlParserPos.ZERO);
            
            Set<WideTableSubqueryColumnDO> leftSubQueryCols = new HashSet<>();
            Map<ColumnKey, List<TraceColumn>> newLeftTraceColMapping = new HashMap<>();
            for (Map.Entry<ColumnKey, SqlNode> entry : leftSqlNodeListColMapping.entrySet()) {
                SqlNode node = entry.getValue();
                newSqlNodeList.add(node);
                
                SubQueryColumn subQueryCol = new SubQueryColumn(new WideTableSubqueryColumnDO(), node);
                ColumnKey colKey = subQueryCol.getColumnKey();
                setColBasic(subQueryCol);
                putToList(
                        newLeftTraceColMapping,
                        colKey,
                        newTraceCol(subQueryCol, defaultColKeyMapper, leftTraceColMapping, defaultColKeyMapper, TraceColumn.SEMAPHORE_1)
                );
                
                leftSubQueryCols.add(subQueryCol.columnDO);
            }
            
            leftTraceColMapping = newLeftTraceColMapping;
            
            // sql node 变形
            currentSqlSelect.setFrom(newSqlJoin);
            currentSqlSelect.setSelectList(newSqlNodeList);
            currentSqlSelect.setWhere(null);
            currentSqlSelect.setHaving(null);
            
            // 设置子列
            leftSubQuery.setWideTableSubqueryColumns(leftSubQueryCols);
            // 设置 SqlNode 变形后的sql
            leftSubQuery.setSelectStatement(currentSqlSelect.toString());
            // 基础信息设置
            setSubQueryBasic(DEFAULT_RESULT_NAME, currentSqlSelect, leftSubQuery);
            // 设置为虚拟节点
            leftSubQuery.setReal(Boolean.FALSE);
            // 设置级联
            setColumnsCascade(leftSubQuery, leftSubQueryCols);
            
            // sql 还原
            currentSqlSelect.setFrom(sqlJoin);
            currentSqlSelect.setSelectList(originalSqlNodeList);
            currentSqlSelect.setWhere(originalWhere);
            currentSqlSelect.setHaving(originalHaving);
        }
        
        // on 条件处理
        List<SqlBasicCall> predicates = extractPredicate(sqlJoin.getCondition());
        Set<WideTableSubqueryJoinConditionDO> joinConditions = new HashSet<>();
        for (SqlBasicCall predicate : predicates) {
            WideTableSubqueryJoinConditionDO conditionDO = new WideTableSubqueryJoinConditionDO();
            String left = predicateLeftOf(predicate).toString();
            String right = predicateRightOf(predicate).toString();
            conditionDO.setId(idGenerator.id());
            conditionDO.setOperator(predicate.getOperator().getName());
            conditionDO.setLeftColumn(left);
            conditionDO.setRightColumn(right);
            joinConditions.add(conditionDO);
        }
        
        // subQuery 处理
        setConditionCascade(popUpSubQuery, joinConditions);
        popUpSubQuery.setTableSourceType(WideTableSubqueryTableSourceType.JOINED);
        popUpSubQuery.setJoinType(WideTableSqlJoinType.valueOf(sqlJoin.getJoinType().name()));
        popUpSubQuery.setLeftSubquery(leftSubQuery);
        popUpSubQuery.setRightSubquery(rightSubQuery);
        
        // 上下文保存
        lineageColumnStack.push(leftLineageColMapping);
        lineageColumnStack.push(rightLineageColMapping);
        
        traceColumnStack.push(leftTraceColMapping);
        traceColumnStack.push(rightTraceColMapping);
        
        sqlNodeListColumnStack.push(leftSqlNodeListColMapping);
        sqlNodeListColumnStack.push(rightSqlNodeListColMapping);
        
        subQueryStack.push(leftSubQuery);
        subQueryStack.push(rightSubQuery);
        
    }
    
    private void handleAtomQuery(final SqlSelect atomSubQuery, final String alias, final String sql) {
        Map<ColumnKey, List<SubQueryColumn>> popUpLineageColumnMapping = lineageColumnStack.pop();
        Map<ColumnKey, List<TraceColumn>> popUpTraceColMapping = traceColumnStack.pop();
        sqlNodeListColumnStack.pop();
        
        WideTableSubqueryDO popUpSubQuery = subQueryStack.pop();
        // 1.创建列集合
        SqlNodeList sqlNodeList = atomSubQuery.getSelectList();
        String tableSource = getAtomQueryTableSourceNameIgnoreSchema(atomSubQuery);
        Set<WideTableSubqueryColumnDO> tobePersistedCols = new HashSet<>();
        Set<SubQueryColumn> subQueryCols = new HashSet<>();
        for (SqlNode node : sqlNodeList) {
            SubQueryColumn subQueryCol = new SubQueryColumn(new WideTableSubqueryColumnDO(), node);
            
            // 基础信息设置
            setColBasic(subQueryCol);
            
            // 数据类型设置
            setColDataType(tableSource, subQueryCol);
            
            // 唯一索引设置
            setColUniqueIndexesIfNeeded(tableSource, subQueryCol);
            
            // 主键设置
            setColPkIfNeeded(tableSource, subQueryCol);
            
            tobePersistedCols.add(subQueryCol.columnDO);
            subQueryCols.add(subQueryCol);
            
            // 设置字段类型
            TraceColumn traceColum = newTraceCol(
                    subQueryCol,
                    aliasOrNameColKeyMapper,
                    popUpTraceColMapping,
                    nameColKeyMapper,
                    TraceColumn.SEMAPHORE_0
            );
            
            setTraceColDataType(traceColum);
        }
        
        setColLineage(subQueryCols, aliasOrNameColKeyMapper, popUpLineageColumnMapping, nameColKeyMapper);
        //setSubQueryBasic(alias, atomSubQuery, popUpSubQuery);
        // 使用源表名，别名经过 SQL 优化后会改变，即使用户编写 SQL 中指定了别名
        setSubQueryBasic(tableSource, atomSubQuery, popUpSubQuery);
        setColumnsCascade(popUpSubQuery, tobePersistedCols);
        
        popUpSubQuery.setTableSourceType(WideTableSubqueryTableSourceType.ATOM);
        popUpSubQuery.setReal(Boolean.TRUE);
        popUpSubQuery.setSelectStatement(sql);
        popUpSubQuery.setDataSystemResource(getDataSystemResource(tableSource));
    }
    
    private void splitColumn(
            final SubQueryColumn functionCol,
            final TraceColumn traceColumn,
            final Map<ColumnKey, SqlNode> sqlNodeListColMapping,
            final Map<ColumnKey, List<SubQueryColumn>> lineageColMapping,
            final Map<ColumnKey, List<TraceColumn>> traceColMapping
    ) {
        List<SqlIdentifier> subCols = splitFunctionSqlNodeListCol(functionCol.sqlNodeListCol);
        for (SqlIdentifier col : subCols) {
            ColumnKey columnKey = new ColumnKey(col);
            
            // 保存血缘关系列
            putToList(lineageColMapping, columnKey, functionCol);
            
            // 保存跟踪列
            putToList(traceColMapping, columnKey, traceColumn);
            
            // 拆分出的列如果和当前查询的列重复，则优先保留当前查询的列
            putIfAbsent(sqlNodeListColMapping, columnKey, col);
        }
    }
    
    private String getWhereExpression(final SqlSelect sqlSelect) {
        if (Objects.isNull(sqlSelect.getWhere())) {
            return null;
        }
        return sqlSelect.getWhere().toString();
    }
    
    private String getOtherExpression(final SqlSelect sqlSelect) {
        StringBuilder expression = new StringBuilder();
        
        if (Objects.nonNull(sqlSelect.getHaving())) {
            expression.append(sqlSelect.getHaving());
        }
        
        if (Strings.isNullOrEmpty(expression.toString())) {
            return null;
        }
        
        return expression.toString();
    }
    
    private void setTraceColDataType(final TraceColumn traceColumn) {
 /*       if (Strings.isNullOrEmpty(traceColumn.dataType)) {
            String errorMessage = String.format("An exception occurred during," +
                            "processing data type column, The data type is null: %s",
                    traceColumn
            );
            throw new AcdcServiceException(errorMessage);
        }*/
        
        // 递归边界:child 为空，(说明：已经到达了 select 的最外层查询列)
        if (!CollectionUtils.isEmpty(traceColumn.children)) {
            for (TraceColumn each : traceColumn.children) {
                each.addDataType(traceColumn);
                setTraceColDataType(each);
            }
        }
    }
    
    private void setColLineage(
            final Set<SubQueryColumn> subQueryCols,
            final Function<ColumnKey, Object> detailColMapper,
            final Map<ColumnKey, List<SubQueryColumn>> lineageColMapping,
            final Function<ColumnKey, Object> lineageColMapper
    ) {
        Map<Object, List<SubQueryColumn>> remappedLineageColMapping = remap(lineageColMapping, lineageColMapper);
        for (SubQueryColumn each : subQueryCols) {
            Object mappedKey = detailColMapper.apply(each.getColumnKey());
            if (!remappedLineageColMapping.containsKey(mappedKey)) {
                continue;
            }
            
            List<SubQueryColumn> lineageCols = remappedLineageColMapping.get(mappedKey);
            for (SubQueryColumn lineageCol : lineageCols) {
                WideTableSubqueryColumnLineageDO lineage = new WideTableSubqueryColumnLineageDO();
                lineage.setId(idGenerator.id());
                lineage.setColumn(lineageCol.columnDO);
                lineage.setParentColumn(each.columnDO);
                // 添加血缘关系，如果一个字段来自多个父级，会被分配到不同的 atomSubQuery 回调函数中处理
                lineageCol.columnDO.getWideTableSubqueryColumnLineages().add(lineage);
            }
        }
    }
    
    private TraceColumn newTraceCol(
            final SubQueryColumn subQueryCol,
            final Function<ColumnKey, Object> colMapper,
            final Map<ColumnKey, List<TraceColumn>> traceColMapping,
            final Function<ColumnKey, Object> traceColMapper,
            final Integer semaphore
    ) {
        Map<Object, List<TraceColumn>> remappedTraceColMapping = remap(traceColMapping, traceColMapper);
        Object mappedKey = colMapper.apply(subQueryCol.getColumnKey());
        
        if (!remappedTraceColMapping.containsKey(mappedKey)) {
            return new TraceColumn(subQueryCol, semaphore);
        }
        
        List<TraceColumn> traceCols = remappedTraceColMapping.get(mappedKey);
        TraceColumn newTraceCol = new TraceColumn(subQueryCol, semaphore);
        for (TraceColumn child : traceCols) {
            newTraceCol.addChild(child);
        }
        return newTraceCol;
    }
    
    private DataSystemResourceDO getDataSystemResource(final String tableSource) {
        if (Objects.isNull(dataSystemResourceMap.get(tableSource))) {
            throw new EntityNotFoundException(i18n.msg(I18nKey.WideTable.DATA_SYSTEM_NOT_FOUND, tableSource));
        }
        return this.dataSystemResourceMap.get(tableSource).toDO();
    }
    
    private DataFieldDefinition getDataFieldDefinition(
            final String tableSource,
            final String fieldName
    ) {
        if (Objects.isNull(dataFieldDefinitionTable.get(tableSource))) {
            throw new EntityNotFoundException(i18n.msg(I18nKey.WideTable.DATA_SYSTEM_NOT_FOUND, tableSource));
        }
        return dataFieldDefinitionTable
                .get(tableSource)
                .get(fieldName);
    }
    
    private String getDataType(
            final String tableSource,
            final String fieldName
    ) {
        String lowerTableSource = tableSource.toLowerCase();
        String lowerFieldName = fieldName.toLowerCase();
        DataFieldDefinition dataFieldDefinition = getDataFieldDefinition(lowerTableSource, lowerFieldName);
        return Objects.isNull(dataFieldDefinition) ? SystemConstant.EMPTY_STRING : dataFieldDefinition.getType();
    }
    
    private Set<String> getUKs(
            final String tableSource,
            final String fieldName
    ) {
        String lowerTableSource = tableSource.toLowerCase();
        String lowerFieldName = fieldName.toLowerCase();
        DataFieldDefinition dataFieldDefinition = getDataFieldDefinition(lowerTableSource, lowerFieldName);
        return Objects.isNull(dataFieldDefinition) ? Collections.EMPTY_SET : dataFieldDefinition.getUniqueIndexNames();
        
    }
    
    private String getFunctionColDataType(
            final List<SqlIdentifier> columns,
            final String tableSource
    ) {
        List<String> dataTypes = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            SqlIdentifier column = columns.get(i);
            String dataFieldName = columnRightStringOf(column);
            String dataType = getDataType(tableSource, dataFieldName);
            if (!Strings.isNullOrEmpty(dataType)) {
                dataTypes.add(dataType);
            }
        }
        
        if (CollectionUtils.isEmpty(dataTypes)) {
            return SystemConstant.EMPTY_STRING;
        }
        
        return Joiner.on(SystemConstant.Symbol.COMMA).join(dataTypes);
    }
    
    private void setColUniqueIndexesIfNeeded(
            final String tableSource,
            final SubQueryColumn subQueryColumn
    ) {
        // 函数类型字段不可以设置唯一索引标识
        if (isFunctionSqlNodeListCol(subQueryColumn.sqlNodeListCol)) {
            return;
        }
        
        ColumnKey columnKey = subQueryColumn.getColumnKey();
        WideTableSubqueryColumnDO columnDO = subQueryColumn.columnDO;
        
        String dataFieldName = columnKey.name;
        Set<String> uniqueIndexNames = getUKs(tableSource, dataFieldName);
        Set<WideTableSubqueryColumnUniqueIndexDO> uniqueIndexes = new HashSet<>();
        for (String name : uniqueIndexNames) {
            WideTableSubqueryColumnUniqueIndexDO uniqueIndex = new WideTableSubqueryColumnUniqueIndexDO()
                    .setId(idGenerator.id())
                    .setName(name)
                    .setColumn(columnDO);
            
            uniqueIndexes.add(uniqueIndex);
        }
        
        columnDO.setWideTableSubqueryColumnUniqueIndex(uniqueIndexes);
    }
    
    private void setColPkIfNeeded(
            final String tableSource,
            final SubQueryColumn subQueryColumn
    
    ) {
        // TODO 取消主键字段配置
        subQueryColumn.columnDO.setPrimaryKey(Boolean.FALSE);
    }
    
    private void setColBasic(
            final SubQueryColumn subqueryColumn
    ) {
        ColumnKey columnKey = subqueryColumn.getColumnKey();
        subqueryColumn.columnDO.setId(idGenerator.id());
        subqueryColumn.columnDO.setAlias(columnKey.alias);
        subqueryColumn.columnDO.setExpression(columnKey.expression);
    }
    
    private void setSubQueryBasic(
            final String name,
            final SqlSelect sqlSelect,
            final WideTableSubqueryDO subQuery
    ) {
        subQuery.setId(idGenerator.id());
        subQuery.setName(name);
        subQuery.setWhereExpression(getWhereExpression(sqlSelect));
        subQuery.setOtherExpression(getOtherExpression(sqlSelect));
    }
    
    private void setColDataType(
            final String tableSource,
            final SubQueryColumn subQueryColumn
    ) {
        ColumnKey columnKey = subQueryColumn.getColumnKey();
        if (!isFunctionSqlNodeListCol(subQueryColumn.sqlNodeListCol)) {
            String dataFieldName = columnKey.name;
            subQueryColumn.columnDO.setType(getDataType(tableSource, dataFieldName));
        } else {
            List<SqlIdentifier> subColumns = splitFunctionSqlNodeListCol(subQueryColumn.sqlNodeListCol);
            subQueryColumn.columnDO.setType(getFunctionColDataType(subColumns, tableSource));
        }
    }
    
    private <K, V> Pair<Map<ColumnKey, V>, Map<ColumnKey, V>> binarySplit(
            final String tableSourceAlias,
            final Map<ColumnKey, V> toBesplitMap
    ) {
        Preconditions.checkNotNull(tableSourceAlias, "The table source alias must not be null!");
        
        Map<ColumnKey, V> right = new HashMap<>();
        
        Map<ColumnKey, V> left = new HashMap<>();
        
        if (CollectionUtils.isEmpty(toBesplitMap)) {
            return Pair.of(left, right);
        }
        
        left = new HashMap<>(toBesplitMap);
        Iterator<Map.Entry<ColumnKey, V>> iterator = left.entrySet().iterator();
        for (; iterator.hasNext(); ) {
            Map.Entry<ColumnKey, V> entry = iterator.next();
            ColumnKey columnKey = entry.getKey();
            V value = entry.getValue();
            if (columnKey.tableSourceAlias.equals(tableSourceAlias)) {
                right.put(columnKey, value);
                iterator.remove();
            }
        }
        
        return Pair.of(left, right);
    }
    
    private <E> void putToList(
            final Map<ColumnKey, List<E>> map,
            final ColumnKey key,
            final E e
    ) {
        List<E> list = map
                .computeIfAbsent(key, value -> new ArrayList<>());
        
        list.add(e);
    }
    
    private <E> void putAndCheckIfExist(
            final Map<ColumnKey, E> map,
            final ColumnKey key,
            final E e
    ) {
        if (map.containsKey(key)) {
            throw new AcdcServiceException("Duplicate query columns");
        }
        map.put(key, e);
    }
    
    private <E> void putIfAbsent(
            final Map<ColumnKey, E> map,
            final ColumnKey key,
            final E e
    ) {
        map.putIfAbsent(key, e);
    }
    
    private <E> Map<Object, E> remap(
            final Map<ColumnKey, E> map,
            final Function<ColumnKey, Object> colKeyMapper
    ) {
        Map<Object, E> newMap = new HashMap<>();
        for (Map.Entry<ColumnKey, E> entry : map.entrySet()) {
            Object newKey = colKeyMapper.apply(entry.getKey());
            E value = entry.getValue();
            newMap.put(newKey, value);
        }
        return newMap;
    }
    
    private void setColumnsCascade(
            final WideTableSubqueryDO subQuery,
            final Set<WideTableSubqueryColumnDO> subQueryColumns
    ) {
        subQuery.setWideTableSubqueryColumns(subQueryColumns);
        for (WideTableSubqueryColumnDO column : subQueryColumns) {
            column.setSubquery(subQuery);
        }
    }
    
    private void setConditionCascade(
            final WideTableSubqueryDO subQuery,
            final Set<WideTableSubqueryJoinConditionDO> conditions
    ) {
        subQuery.setWideTableSubqueryJoinConditions(conditions);
        for (WideTableSubqueryJoinConditionDO condition : conditions) {
            condition.setSubquery(subQuery);
        }
    }
    
    @Data
    @Accessors(chain = true)
    private static class TraceColumn {
        
        private static int SEMAPHORE_0 = 0;
        
        private static int SEMAPHORE_1 = 1;
        
        private SubQueryColumn column;
        
        private List<TraceColumn> children;
        
        private String dataType;
        
        // 函数列存在多个类型，当底层对应的字段冒泡到本层达到需要的个数，才能向上一层冒泡，解决函数列的上一层还有字段，导致最上层字段出现重复问题
        private Integer semaphore;
        
        public TraceColumn(
                final SubQueryColumn column,
                final Integer semaphore
        ) {
            this.column = column;
            this.dataType = column.getColumnDO().getType();
            this.children = new ArrayList<>();
            this.semaphore = semaphore;
        }
        
        private void addDataType(TraceColumn parent) {
            // 父字段还没有匹配完所有的字段类型，则不需要向上一层冒泡，当最后一个字段类型匹配完成才可以向上冒泡
            // 函数类型会出现一对多的情况，eg：c->a,c-b,c-d 函数类型1对多的情况
            if (parent.semaphore != 0) {
                return;
            }
            
            String pDataType = parent.dataType;
            String type = column.columnDO.getType();
            if (Strings.isNullOrEmpty(type)) {
                column.columnDO.setType(pDataType);
            } else {
                column.columnDO.setType(type + SystemConstant.Symbol.COMMA + pDataType);
            }
            
            semaphore--;
            this.dataType = column.getColumnDO().getType();
        }
        
        private void addChild(TraceColumn child) {
            this.children.add(child);
        }
    }
    
    @Data
    private class SubQueryColumn {
        
        private WideTableSubqueryColumnDO columnDO;
        
        private SqlNode sqlNodeListCol;
        
        public SubQueryColumn(
                final WideTableSubqueryColumnDO columnDO,
                final SqlNode sqlNodeListCol
        ) {
            this.columnDO = columnDO;
            this.sqlNodeListCol = sqlNodeListCol;
        }
        
        private ColumnKey getColumnKey() {
            return new ColumnKey(sqlNodeListCol);
        }
    }
    
    @Data
    private final class ColumnKey {
        
        private final String tableSourceAlias;
        
        private final String name;
        
        private final String alias;
        
        private final String expression;
        
        public ColumnKey(final SqlNode sqlNodeListCol) {
            this.name = getSqlNodeListColName(sqlNodeListCol);
            this.alias = getSqlNodeListColAlias(sqlNodeListCol);
            this.tableSourceAlias = getSqlNodeListColTableSourceAlias(sqlNodeListCol);
            this.expression = getSqlNodeListColExpression(sqlNodeListCol);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ColumnKey columnKey = (ColumnKey) o;
            return Objects.equals(tableSourceAlias, columnKey.tableSourceAlias) && Objects.equals(name, columnKey.name);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(tableSourceAlias, name);
        }
    }
    
}
