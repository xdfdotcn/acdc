package cn.xdf.acdc.devops.service.process.widetable.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnLineageDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSubqueryTableSourceType;
import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.widetable.IdGenerator;
import cn.xdf.acdc.devops.service.process.widetable.sql.MySqlSqlParser;
import cn.xdf.acdc.devops.service.process.widetable.sql.SqlUtil;
import cn.xdf.acdc.devops.service.process.widetable.sql.WideTableSqlService;

// 1. 原子查询 完成
// 2. 子查询 完成
// 3. 普通join 完成
// 4. join中带有 join 子查询 完成
// 5. join 中带有from中存在子查询 完成
// 6. 函数列，单函数 完成
// 7. 函数列，多列组合函数 完成
// CHECKSTYLE:OFF
@RunWith(SpringRunner.class)
public class SubQueryPersistenceSqlNodeHandlerTest {
    
    private static final Set<Long> TABLE_RESOURCE_IDS = Sets.newHashSet(1L, 2L, 3L, 4L);
    
    @Mock
    private DataSystemResourceService dataSystemResourceService;
    
    @Mock
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Mock
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Mock
    private WideTableSqlService wideTableSqlService;
    
    private WideTableServiceImpl wideTableServiceImpl;
    
    @Before
    public void setUp() throws Exception {
        wideTableServiceImpl = new WideTableServiceImpl();
        ReflectionTestUtils.setField(wideTableServiceImpl, "dataSystemResourceService", dataSystemResourceService);
        ReflectionTestUtils.setField(wideTableServiceImpl, "dataSystemServiceManager", dataSystemServiceManager);
        ReflectionTestUtils.setField(wideTableServiceImpl, "wideTableSqlService", wideTableSqlService);
        mockTableSource();
    }
    
    @Test
    public void testAtomQuery() throws SqlParseException {
        String sql = "SELECT student.id AS studentId, student.student_name  AS studentName  FROM student_3 AS student " +
                "WHERE student.id>=1 AND student.id<=10 GROUP BY student.id HAVING student.id<=11";
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql,
                TABLE_RESOURCE_IDS
        
        );
        
        SubQueryTracker tracker = newTracker(subQuery);
        
        shouldBeRealNode(sql, tracker);
        shouldBeAtom(sql, tracker);
        verifySubQueryCol(sql, tracker, "student.id|int(11)", "student.student_name|varchar(45)");
        verifyWhereExpression(sql, tracker, "student.id>=1 AND student.id<=10");
        verifyOtherExpression(sql, tracker, "student.id<=11");
        verifyColLineage(tracker);
    }
    
    @Test
    public void testNestedSubQuery() throws SqlParseException {
        String sql1 = "SELECT student1.id AS studentId, student1.student_name AS studentName\n" +
                "                FROM (SELECT student0.id AS id,student0.student_name\n" +
                "                FROM student_3 AS student0) AS student1";
        
        String sql2 = "SELECT student0.id AS id,student0.student_name\n" +
                "FROM student_3 AS student0";
        
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql1));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql1,
                TABLE_RESOURCE_IDS
        );
        SubQueryTracker tracker = newTracker(subQuery);
        
        // sql 关系校验
        verifyRelation(sql1, sql2, RelationType.SUB_QUERY, tracker);
        
        // sql
        shouldBeRealNode(sql1, tracker);
        shouldBeSubQuery(sql1, tracker);
        verifySubQueryCol(sql1, tracker, "student1.id|int(11)", "student1.student_name|varchar(45)");
        verifyWhereExpression(sql1, tracker, null);
        verifyOtherExpression(sql1, tracker, null);
        
        // sql1
        shouldBeRealNode(sql2, tracker);
        shouldBeAtom(sql2, tracker);
        verifySubQueryCol(sql2, tracker, "student0.id|int(11)", "student0.student_name|varchar(45)");
        verifyWhereExpression(sql2, tracker, null);
        verifyOtherExpression(sql2, tracker, null);
        
        verifyColLineage(tracker, "student1.id->student0.id", "student1.student_name->student0.student_name");
    }
    
    @Test
    public void testJoinedQuery() throws SqlParseException {
        String sql1 = "SELECT student.id AS studentId, class.id AS classId, SUBSTRING(class.id, 1) " +
                "FROM(SELECT student0.id AS id, student0.class_id AS class_id FROM student_3 AS student0 WHERE id = 1) AS student " +
                "LEFT JOIN (SELECT class0.id FROM class_2 AS class0 GROUP BY class0.id HAVING class0.id = 1 ORDER BY class0.id DESC) AS class " +
                "ON student.class_id = class.id AND student.id>1 " +
                "WHERE student.id = 1 AND student.class_id = 1";
        
        String sql2 = "SELECT class0.id FROM class_2 AS class0 GROUP BY class0.id HAVING class0.id = 1 ORDER BY class0.id DESC";
        String sql3 = "SELECT student0.id AS id, student0.class_id AS class_id FROM student_3 AS student0 WHERE id = 1";
        
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql1));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql1,
                TABLE_RESOURCE_IDS
        );
        SubQueryTracker tracker = newTracker(subQuery);
        
        // sql 关系校验
        verifyRelation(sql1, sql2, RelationType.RIGHT, tracker);
        verifyRelation(sql1, sql3, RelationType.LEFT, tracker);
        
        // sql
        shouldBeRealNode(sql1, tracker);
        shouldBeJoined(sql1, tracker);
        verifySubQueryCol(sql1, tracker, "student.id|int(11)", "class.id|int(11)", "SUBSTRING(class.id, 1)|int(11)");
        verifyWhereExpression(sql1, tracker, "student.id = 1 AND student.class_id = 1");
        verifyOtherExpression(sql1, tracker, null);
        
        // sql1
        shouldBeRealNode(sql2, tracker);
        shouldBeAtom(sql2, tracker);
        verifySubQueryCol(sql2, tracker, "class0.id|int(11)");
        verifyWhereExpression(sql2, tracker, null);
        verifyOtherExpression(sql2, tracker, "class0.id = 1");
        
        verifyColLineage(tracker, "student.id->student0.id", "class.id->class0.id", "SUBSTRING(class.id, 1)->class0.id");
    }
    
    @Test
    public void testJoinedQueryWithJoinedSubQuery() throws SqlParseException {
        
        String sql1 = "SELECT student.id AS studentId, student.student_name, class1.id AS classId,student_info.age AS studentAge " +
                "FROM(SELECT student0.id, student0.class_id, student0.student_name FROM student_3 AS student0) AS student " +
                " LEFT JOIN (SELECT class.id AS id, grade.id AS grade_id " +
                "            FROM (SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0) AS class " +
                "            LEFT JOIN (SELECT grade0.id FROM grade_1 AS grade0) AS grade ON class.grade_id = grade.id AND grade.id = 1 AND class.id = 1 AND class.class_name = 'class1') AS class1 " +
                "ON student.class_id = class1.id " +
                "LEFT JOIN (SELECT student_info0.student_id, student_info0.age FROM student_info_4 AS student_info0) AS student_info " +
                "ON student_info.student_id = student.id AND student_info.age > 1 " +
                "WHERE student_info.student_id>=1 AND student_info.student_id<=10 ";
        
        String sql2 = "SELECT student_info0.student_id, student_info0.age FROM student_info_4 AS student_info0";
        
        String sql3 = "SELECT student.id AS studentId, student.student_name, class1.id AS classId " +
                "FROM(SELECT student0.id, student0.class_id, student0.student_name FROM student_3 AS student0) AS student " +
                " LEFT JOIN (SELECT class.id AS id, grade.id AS grade_id " +
                "            FROM (SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0) AS class " +
                "            LEFT JOIN (SELECT grade0.id FROM grade_1 AS grade0) AS grade ON class.grade_id = grade.id AND grade.id = 1 AND class.id = 1 AND class.class_name = 'class1') AS class1 " +
                "ON student.class_id = class1.id ";
        
        String sql4 = "SELECT class.id AS id, grade.id AS grade_id " +
                "            FROM (SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0) AS class " +
                "            LEFT JOIN (SELECT grade0.id FROM grade_1 AS grade0) AS grade ON class.grade_id = grade.id AND grade.id = 1 AND class.id = 1 AND class.class_name = 'class1' ";
        String sql5 = "SELECT grade0.id FROM grade_1 AS grade0";
        
        String sql6 = "SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0";
        
        String sql7 = "SELECT student0.id, student0.class_id, student0.student_name FROM student_3 AS student0";
        
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql1));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql1,
                TABLE_RESOURCE_IDS
        );
        SubQueryTracker tracker = newTracker(subQuery);
        
        
        verifyRelation(sql1, sql2, RelationType.RIGHT, tracker);
        verifyRelation(sql1, sql3, RelationType.LEFT, tracker);
        
        verifyRelation(sql3, sql7, RelationType.LEFT, tracker);
        verifyRelation(sql3, sql4, RelationType.RIGHT, tracker);
        
        verifyRelation(sql4, sql6, RelationType.LEFT, tracker);
        verifyRelation(sql4, sql5, RelationType.RIGHT, tracker);
        
        // sql1
        shouldBeRealNode(sql1, tracker);
        shouldBeJoined(sql1, tracker);
        verifySubQueryCol(sql1, tracker, "student.id|int(11)", "student.student_name|varchar(45)", "class1.id|int(11)", "student_info.age|int(11)");
        verifyWhereExpression(sql1, tracker, "student_info.student_id>=1 AND student_info.student_id<=10");
        verifyOtherExpression(sql1, tracker, null);
        
        // sql2
        shouldBeRealNode(sql2, tracker);
        shouldBeAtom(sql2, tracker);
        verifySubQueryCol(sql2, tracker, "student_info0.student_id|int(11)", "student_info0.age|int(11)");
        verifyWhereExpression(sql2, tracker, null);
        verifyOtherExpression(sql2, tracker, null);
        
        // sql3
        shouldBeNonRealNode(sql3, tracker);
        shouldBeJoined(sql3, tracker);
        verifySubQueryCol(sql3, tracker, "student.id|int(11)", "student.student_name|varchar(45)", "class1.id|int(11)");
        verifyWhereExpression(sql3, tracker, null);
        verifyOtherExpression(sql3, tracker, null);
        
        // sql4
        shouldBeRealNode(sql4, tracker);
        shouldBeJoined(sql4, tracker);
        verifySubQueryCol(sql4, tracker, "class.id|int(11)", "grade.id|int(11)");
        verifyWhereExpression(sql4, tracker, null);
        verifyOtherExpression(sql4, tracker, null);
        
        // sql5
        shouldBeRealNode(sql5, tracker);
        shouldBeAtom(sql5, tracker);
        verifySubQueryCol(sql5, tracker, "grade0.id|int(11)");
        verifyWhereExpression(sql5, tracker, null);
        verifyOtherExpression(sql5, tracker, null);
        
        // sql6
        shouldBeRealNode(sql6, tracker);
        shouldBeAtom(sql6, tracker);
        verifySubQueryCol(sql6, tracker, "class0.id|int(11)", "class0.grade_id|int(11)", "class0.class_name|varchar(45)");
        verifyWhereExpression(sql6, tracker, null);
        verifyOtherExpression(sql6, tracker, null);
        // sql7
        shouldBeRealNode(sql7, tracker);
        shouldBeAtom(sql7, tracker);
        verifySubQueryCol(sql7, tracker, "student0.id|int(11)", "student0.class_id|int(11)", "student0.student_name|varchar(45)");
        verifyWhereExpression(sql7, tracker, null);
        verifyOtherExpression(sql7, tracker, null);
        
        
        verifyColLineage(tracker,
                "student.id->student0.id",
                "student.student_name->student0.student_name",
                "class1.id->class.id",
                "student_info.age->student_info0.age",
                "class.id->class0.id",
                "grade.id->grade0.id"
        );
    }
    
    @Test
    public void testJoinedQueryWithNestedSubQuery() throws SqlParseException {
        String sql1 = "SELECT student.id AS studentId, student.student_name, class1.id AS classId " +
                "FROM(SELECT student0.id, student0.class_id, student0.student_name FROM student_3 AS student0) AS student " +
                "LEFT JOIN (SELECT class.id AS id " +
                "FROM (SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0) AS class) AS class1 " +
                "ON student.class_id = class1.id ";
        
        String sql2 = "SELECT class.id AS id" +
                " FROM (SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0) AS class ";
        
        
        String sql3 = "SELECT class0.id, class0.grade_id, class0.class_name FROM class_2 AS class0";
        
        String sql4 = "SELECT student0.id, student0.class_id, student0.student_name FROM student_3 AS student0";
        
        
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql1));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql1,
                TABLE_RESOURCE_IDS
        );
        SubQueryTracker tracker = newTracker(subQuery);
        
        verifyRelation(sql1, sql4, RelationType.LEFT, tracker);
        verifyRelation(sql1, sql2, RelationType.RIGHT, tracker);
        verifyRelation(sql2, sql3, RelationType.SUB_QUERY, tracker);
        
        // sql1
        shouldBeRealNode(sql1, tracker);
        shouldBeJoined(sql1, tracker);
        verifySubQueryCol(sql1, tracker, "student.id|int(11)", "class1.id|int(11)", "student.student_name|varchar(45)");
        verifyWhereExpression(sql1, tracker, null);
        verifyOtherExpression(sql1, tracker, null);
        
        // sql2
        shouldBeRealNode(sql2, tracker);
        shouldBeSubQuery(sql2, tracker);
        verifySubQueryCol(sql2, tracker, "class.id|int(11)");
        verifyWhereExpression(sql2, tracker, null);
        verifyOtherExpression(sql2, tracker, null);
        
        // sql3
        shouldBeRealNode(sql3, tracker);
        shouldBeAtom(sql3, tracker);
        verifySubQueryCol(sql3, tracker, "class0.id|int(11)", "class0.grade_id|int(11)", "class0.class_name|varchar(45)");
        verifyWhereExpression(sql3, tracker, null);
        verifyOtherExpression(sql3, tracker, null);
        
        // sql4
        shouldBeRealNode(sql4, tracker);
        shouldBeAtom(sql4, tracker);
        verifySubQueryCol(sql4, tracker, "student0.id|int(11)", "student0.class_id|int(11)", "student0.student_name|varchar(45)");
        verifyWhereExpression(sql4, tracker, null);
        verifyOtherExpression(sql4, tracker, null);
        
        verifyColLineage(tracker,
                "student.id->student0.id",
                "student.student_name->student0.student_name",
                "class1.id->class.id",
                "class.id->class0.id"
        );
    }
    
    @Test
    public void testFunctionColumnSplit() throws SqlParseException {
        String sql1 = "SELECT student.id AS studentId, class.id AS classId, CONCAT(class.id, '-', student.id) AS classAndStudentId " +
                "FROM(SELECT student0.id AS id, student0.class_id AS class_id FROM student_3 AS student0 WHERE id = 1) AS student " +
                "LEFT JOIN (SELECT class0.id FROM class_2 AS class0 GROUP BY class0.id HAVING class0.id = 1 ORDER BY class0.id DESC) AS class " +
                "ON student.class_id = class.id AND student.id>1 " +
                "WHERE student.id = 1 AND student.class_id = 1";
        
        String sql2 = "SELECT class0.id FROM class_2 AS class0 GROUP BY class0.id HAVING class0.id = 1 ORDER BY class0.id DESC";
        String sql3 = "SELECT student0.id AS id, student0.class_id AS class_id FROM student_3 AS student0 WHERE id = 1";
        
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql1));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql1,
                TABLE_RESOURCE_IDS
        );
        SubQueryTracker tracker = newTracker(subQuery);
        
        // sql 关系校验
        verifyRelation(sql1, sql2, RelationType.RIGHT, tracker);
        verifyRelation(sql1, sql3, RelationType.LEFT, tracker);
        
        // sql
        shouldBeRealNode(sql1, tracker);
        shouldBeJoined(sql1, tracker);
        verifySubQueryCol(sql1, tracker, "student.id|int(11)", "class.id|int(11)", "CONCAT(class.id, '-', student.id)|int(11),int(11)");
        verifyWhereExpression(sql1, tracker, "student.id = 1 AND student.class_id = 1");
        verifyOtherExpression(sql1, tracker, null);
        
        // sql1
        shouldBeRealNode(sql2, tracker);
        shouldBeAtom(sql2, tracker);
        verifySubQueryCol(sql2, tracker, "class0.id|int(11)");
        verifyWhereExpression(sql2, tracker, null);
        verifyOtherExpression(sql2, tracker, "class0.id = 1");
        
        
        verifyColLineage(
                tracker,
                "student.id->student0.id",
                "class.id->class0.id",
                "CONCAT(class.id, '-', student.id)->class0.id",
                "CONCAT(class.id, '-', student.id)->student0.id"
        );
    }
    
    @Test
    public void testNestedFunctionColumn() throws SqlParseException {
        String sql1 = "SELECT CONCAT(student.classIdAndStudentId, '-', student.student_name) as classIdAndStudentIdAndStudentName " +
                "FROM(SELECT CONCAT(student0.class_id, '-', student0.id) as classIdAndStudentId, " +
                "student0.id as id,student0.class_id as class_id,student0.student_name as student_name FROM student_3 AS student0 WHERE id = 1) AS student " +
                "LEFT JOIN (SELECT class0.id as id FROM class_2 AS class0) AS class " +
                " on student.class_id=class.id";
        when(wideTableSqlService.validate(any(), any())).thenReturn(MySqlSqlParser.parse(sql1));
        WideTableSubqueryDO subQuery = wideTableServiceImpl.generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                sql1,
                TABLE_RESOURCE_IDS
        );
        SubQueryTracker tracker = newTracker(subQuery);
        
        verifySubQueryCol(sql1, tracker, "CONCAT(student.classIdAndStudentId, '-', student.student_name)|int(11),int(11),varchar(45)");
    }
    
    private void mockTableSource() {
        List<DataFieldDefinition> yGradeFields = Lists.newArrayList(
                new DataFieldDefinition("id", "int(11)", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("grade_name", "varchar(45)", Sets.newHashSet()),
                new DataFieldDefinition("grade_description", "varchar(45)", Sets.newHashSet())
        );
        DataCollectionDefinition yGrade = new DataCollectionDefinition("grade", yGradeFields);
        
        List<DataFieldDefinition> yClassFields = Lists.newArrayList(
                new DataFieldDefinition("id", "int(11)", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("class_name", "varchar(45)", Sets.newHashSet("class_name_idx")),
                new DataFieldDefinition("class_description", "varchar(45)", Sets.newHashSet()),
                new DataFieldDefinition("grade_id", "int(11)", Sets.newHashSet())
        );
        DataCollectionDefinition yClass = new DataCollectionDefinition("class", yClassFields);
        
        List<DataFieldDefinition> yStudentFields = Lists.newArrayList(
                new DataFieldDefinition("id", "int(11)", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("student_name", "varchar(45)", Sets.newHashSet()),
                new DataFieldDefinition("student_description", "varchar(45)", Sets.newHashSet()),
                new DataFieldDefinition("class_id", "int(11)", Sets.newHashSet())
        );
        DataCollectionDefinition yStudent = new DataCollectionDefinition("student", yStudentFields);
        
        List<DataFieldDefinition> yStudentInfoFields = Lists.newArrayList(
                new DataFieldDefinition("id", "int(11)", Sets.newHashSet("PRIMARY")),
                new DataFieldDefinition("student_id", "int(11)", Sets.newHashSet()),
                new DataFieldDefinition("phone", "varchar(45)", Sets.newHashSet()),
                new DataFieldDefinition("age", "int(11)", Sets.newHashSet()),
                new DataFieldDefinition("student_name", "varchar(45)", Sets.newHashSet())
        );
        DataCollectionDefinition yStudentInfo = new DataCollectionDefinition("student_info", yStudentInfoFields);
        
        when(dataSystemServiceManager.getDataSystemMetadataService(any()))
                .thenReturn(dataSystemMetadataService);
        
        when(dataSystemMetadataService.getDataCollectionDefinition(1L))
                .thenReturn(yGrade);
        
        when(dataSystemMetadataService.getDataCollectionDefinition(2L))
                .thenReturn(yClass);
        
        when(dataSystemMetadataService.getDataCollectionDefinition(3L))
                .thenReturn(yStudent);
        
        when(dataSystemMetadataService.getDataCollectionDefinition(4L))
                .thenReturn(yStudentInfo);
        
        
        when(dataSystemResourceService.getById(eq(1L)))
                .thenReturn(new DataSystemResourceDTO().setId(1L).setName("grade"));
        
        when(dataSystemResourceService.getById(eq(2L)))
                .thenReturn(new DataSystemResourceDTO().setId(2L).setName("class"));
        
        when(dataSystemResourceService.getById(eq(3L)))
                .thenReturn(new DataSystemResourceDTO().setId(3L).setName("student"));
        
        when(dataSystemResourceService.getById(eq(4L)))
                .thenReturn(new DataSystemResourceDTO().setId(4L).setName("student_info"));
    }
    
    private SubQueryTracker newTracker(final WideTableSubqueryDO subQuery) {
        Map<Long, WideTableSubqueryColumnDO> colIdToColExpression = new HashMap<>();
        Map<String, WideTableSubqueryColumnDO> colExpressionToColId = new HashMap<>();
        Map<String, WideTableSubqueryDO> sqlToSubQuery = new HashMap<>();
        
        Queue<WideTableSubqueryDO> subQueryQueue = new LinkedList<>();
        subQueryQueue.offer(subQuery);
        
        for (; !subQueryQueue.isEmpty(); ) {
            WideTableSubqueryDO child = subQueryQueue.poll();
            
            sqlToSubQuery.put(SqlUtil.format(child.getSelectStatement()), child);
            if (child.getReal()) {
                for (WideTableSubqueryColumnDO column : child.getWideTableSubqueryColumns()) {
                    String exp = SqlUtil.format(column.getExpression());
                    Long id = column.getId();
                    if (colIdToColExpression.containsKey(id)
                            || colExpressionToColId.containsKey(exp)) {
                        throw new AcdcServiceException("Duplicate key");
                    }
                    colIdToColExpression.put(id, column);
                    colExpressionToColId.put(exp, column);
                }
            }
            
            if (Objects.nonNull(child.getLeftSubquery())) {
                subQueryQueue.offer(child.getLeftSubquery());
            }
            if (Objects.nonNull(child.getRightSubquery())) {
                subQueryQueue.offer(child.getRightSubquery());
            }
            if (Objects.nonNull(child.getSubquery())) {
                subQueryQueue.offer(child.getSubquery());
            }
        }
        
        return new SubQueryTracker(sqlToSubQuery, colIdToColExpression, colExpressionToColId);
    }
    
    private void verifySubQueryCol(
            final String sql,
            final SubQueryTracker tracker,
            final String... expectCols
    ) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Map<String, WideTableSubqueryColumnDO> expressionToCol = subQuery.getWideTableSubqueryColumns().stream()
                .collect(Collectors.toMap(it -> SqlUtil.format(it.getExpression()), it -> it));
        
        Assertions.assertThat(subQuery.getWideTableSubqueryColumns().size())
                .isEqualTo(expectCols.length);
        
        for (String col : expectCols) {
            List<String> splitedList = Splitter.on("|").splitToList(col);
            String expression = SqlUtil.format(splitedList.get(0).trim());
            String type = splitedList.get(1).trim();
            
            Assertions.assertThat(expressionToCol.containsKey(expression)).isTrue();
            
            Assertions.assertThat(expressionToCol.get(expression).getType())
                    .isEqualTo(type);
        }
    }
    
    private void verifyRelation(final String sql1, final String sql2, RelationType type, SubQueryTracker tracker) {
        WideTableSubqueryDO subQuery1 = tracker.getSubQueryBySql(sql1);
        WideTableSubqueryDO subQuery2 = tracker.getSubQueryBySql(sql2);
        switch (type) {
            case LEFT:
                Assertions.assertThat(subQuery1.getLeftSubquery()).isEqualTo(subQuery2);
                break;
            case RIGHT:
                Assertions.assertThat(subQuery1.getRightSubquery()).isEqualTo(subQuery2);
                break;
            case SUB_QUERY:
                Assertions.assertThat(subQuery1.getSubquery()).isEqualTo(subQuery2);
                break;
            default:
                throw new IllegalStateException();
        }
    }
    
    private void verifyWhereExpression(
            final String sql,
            final SubQueryTracker tracker,
            final String expectExpression
    ) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(SqlUtil.equals(subQuery.getWhereExpression(), expectExpression)).isTrue();
    }
    
    private void verifyOtherExpression(
            final String sql,
            final SubQueryTracker tracker,
            final String expectExpression
    ) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(SqlUtil.equals(subQuery.getOtherExpression(), expectExpression)).isTrue();
    }
    
    private void verifyColLineage(final SubQueryTracker tracker, final String... logColExpArr) {
        Long expectLigColExpCount = Objects.isNull(logColExpArr) ? 0L : logColExpArr.length;
        Long actualLigColExpCount = 0L;
        for (WideTableSubqueryColumnDO col : tracker.idToCol.values()) {
            actualLigColExpCount += col.getWideTableSubqueryColumnLineages().size();
        }
        
        Assertions.assertThat(actualLigColExpCount).isEqualTo(expectLigColExpCount);
        
        for (String ligColExp : logColExpArr) {
            List<String> splitedList = Splitter.on("->").splitToList(ligColExp);
            String child = SqlUtil.format(splitedList.get(0).trim());
            String parent = SqlUtil.format(splitedList.get(1).trim());
            WideTableSubqueryColumnDO col = tracker.getColByExpression(child);
            WideTableSubqueryColumnDO pCol = tracker.getColByExpression(parent);
            Set<WideTableSubqueryColumnLineageDO> lineages = col.getWideTableSubqueryColumnLineages();
            
            boolean checkPass = false;
            for (WideTableSubqueryColumnLineageDO lineage : lineages) {
                if (lineage.getColumn().getId() == col.getId()
                        && lineage.getParentColumn().getId() == pCol.getId()
                ) {
                    checkPass = true;
                    break;
                }
            }
            Assertions.assertThat(checkPass).isTrue();
        }
    }
    
    private void shouldBeAtom(String sql, SubQueryTracker tracker) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(subQuery.getTableSourceType()).isEqualTo(WideTableSubqueryTableSourceType.ATOM);
        Assertions.assertThat(subQuery.getLeftSubquery()).isNull();
        Assertions.assertThat(subQuery.getRightSubquery()).isNull();
        Assertions.assertThat(subQuery.getSubquery()).isNull();
    }
    
    private void shouldBeJoined(String sql, SubQueryTracker tracker) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(subQuery.getTableSourceType()).isEqualTo(WideTableSubqueryTableSourceType.JOINED);
        Assertions.assertThat(subQuery.getLeftSubquery()).isNotNull();
        Assertions.assertThat(subQuery.getRightSubquery()).isNotNull();
        Assertions.assertThat(subQuery.getSubquery()).isNull();
    }
    
    private void shouldBeSubQuery(String sql, SubQueryTracker tracker) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(subQuery.getTableSourceType()).isEqualTo(WideTableSubqueryTableSourceType.SUBQUERY);
        Assertions.assertThat(subQuery.getLeftSubquery()).isNull();
        Assertions.assertThat(subQuery.getRightSubquery()).isNull();
        Assertions.assertThat(subQuery.getSubquery()).isNotNull();
    }
    
    private void shouldBeRealNode(String sql, SubQueryTracker tracker) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(subQuery.getReal()).isTrue();
    }
    
    private void shouldBeNonRealNode(String sql, SubQueryTracker tracker) {
        WideTableSubqueryDO subQuery = tracker.getSubQueryBySql(sql);
        Assertions.assertThat(subQuery.getReal()).isFalse();
    }
    
    private enum RelationType {
        SUB_QUERY, LEFT, RIGHT
    }
    
    private static class SubQueryTracker {
        
        private Map<String, WideTableSubqueryDO> sqlToSubQuery;
        
        private Map<Long, WideTableSubqueryColumnDO> idToCol;
        
        private Map<String, WideTableSubqueryColumnDO> expressionToCol;
        
        private SubQueryTracker(
                Map<String, WideTableSubqueryDO> sqlToSubQuery,
                Map<Long, WideTableSubqueryColumnDO> idToCol,
                Map<String, WideTableSubqueryColumnDO> expressionToCol
        ) {
            this.sqlToSubQuery = sqlToSubQuery;
            this.idToCol = idToCol;
            this.expressionToCol = expressionToCol;
        }
        
        private WideTableSubqueryDO getSubQueryBySql(final String sql) {
            String formatSql = SqlUtil.format(sql);
            
            Assert.assertTrue(sqlToSubQuery.containsKey(formatSql));
            
            return sqlToSubQuery.get(SqlUtil.format(formatSql));
        }
        
        private WideTableSubqueryColumnDO getColById(final Long id) {
            Assert.assertTrue(idToCol.containsKey(id));
            
            return idToCol.get(id);
        }
        
        private WideTableSubqueryColumnDO getColByExpression(final String expression) {
            String formatExpression = SqlUtil.format(expression);
            Assert.assertTrue(expressionToCol.containsKey(formatExpression));
            
            return expressionToCol.get(formatExpression);
        }
    }
}
