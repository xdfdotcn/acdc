package cn.xdf.acdc.devops.service.process.widetable;

import cn.xdf.acdc.devops.service.process.widetable.sql.SchemasAndSqls;
import cn.xdf.acdc.devops.service.process.widetable.sql.SqlFileToSchemaAndSqlWithName;
import cn.xdf.acdc.devops.service.process.widetable.sql.WideTableSqlService;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.util.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = WideTableSqlServiceTest.Config.class)
public class WideTableSqlServiceTest {
    
    @Autowired
    private WideTableSqlService wideTableSqlService;
    
    @Test
    public void testSimpleSelectShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("select"));
    }
    
    @Test(expected = ClassCastException.class)
    public void testSimpleInsertShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("insert"));
    }
    
    @Test(expected = ClassCastException.class)
    public void testSimpleDeleteShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("delete"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSelectWithScalarQueryInWhereClauseInSingleTableShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectWithScalarQueryInWhere"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSelectWithSelectInWhereClauseInSingleTableShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectWithSelectInWhere"));
    }
    
    @Test
    public void testSimpleAsShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectWithAs"));
    }
    
    @Test
    public void testSimpleGroupByShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectWithGroupBy"));
    }
    
    //@Test
    private void testSimpleSubQueryShouldRetainNeededColumns() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        SqlNode sqlNode = wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectWithSubQueryForCheckColumnRetain"));
        SqlSelect innerSelect = (SqlSelect) ((SqlBasicCall) ((SqlSelect) sqlNode).getFrom()).getOperandList().get(0);
        Assert.equals(2, innerSelect.getSelectList().size());
        Assert.equals("name", ((SqlIdentifier) innerSelect.getSelectList().get(0)).names.get(1));
        Assert.equals("code", ((SqlIdentifier) innerSelect.getSelectList().get(1)).names.get(1));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSelectWithoutInnerTableFieldShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectWithoutInnerTableField"));
    }
    
    @Test
    public void testSimpleManyToOneLeftJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleManyToOneLeftJoinWithOneRelatedKey"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSimpleManyToOneRightJoinWithOneRelatedKeyShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleManyToOneRightJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleManyToOneInnerJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleManyToOneInnerJoinWithOneRelatedKey"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSimpleOneToManyLeftJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleOneToManyLeftJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleOneToManyRightJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleOneToManyRightJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleOneToManyInnerJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleOneToManyInnerJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleOneToOneLeftJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleOneToOneLeftJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleOneToOneRightJoinWithOneRelatedKeyShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleOneToOneRightJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleOneToOneInnerJoinWithOneRelatedKeyShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleOneToOneInnerJoinWithOneRelatedKey"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSimpleManyToOneFullJoinWithOneRelatedKeyShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleManyToOneFullJoinWithOneRelatedKey"));
    }
    
    @Test
    public void testSimpleManyToOneCommaJoinWithOneRelatedKeyShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleManyToOneCommaJoinWithOneRelatedKey"));
    }
    
    //@Test
    private void testsimpleJoinResultShouldRetainNeededColumn() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        SqlNode sqlNode = wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleJoinResultShouldRetainNeededColumn"));
        SqlSelect innerSelect = (SqlSelect) ((SqlBasicCall) ((SqlSelect) sqlNode).getFrom()).getOperandList().get(0);
        Assert.equals(4, innerSelect.getSelectList().size());
        String innerField1 = ((SqlIdentifier) ((SqlBasicCall) innerSelect.getSelectList().get(0)).getOperandList().get(1)).names.get(0);
        String innerField2 = ((SqlIdentifier) ((SqlBasicCall) innerSelect.getSelectList().get(1)).getOperandList().get(1)).names.get(0);
        String innerField3 = ((SqlIdentifier) innerSelect.getSelectList().get(2)).names.get(1);
        String innerField4 = ((SqlIdentifier) ((SqlBasicCall) innerSelect.getSelectList().get(3)).getOperandList().get(1)).names.get(0);
        Assert.equals("order_name", innerField1);
        Assert.equals("order_code", innerField2);
        Assert.equals("user_id", innerField3);
        Assert.equals("user_name", innerField4);
    }
    
    @Test(expected = CalciteContextException.class)
    public void testSimpleJoinWithoutOnShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleJoinWithoutOn"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSimpleJoinWithOrInOnConditionShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("simpleJoinWithOrInOnCondition"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testManyToManyLeftJoinWithMultiRelatedKeysShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToManyLeftJoinWithMultiRelatedKeys"));
    }
    
    @Test
    public void testManyToOneLeftJoinWithMultiRelatedKeysShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneLeftJoinWithMultiRelatedKeys"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testManyToOneRightJoinWithMultiRelatedKeysShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneRightJoinWithMultiRelatedKeys"));
    }
    
    @Test
    public void testManyToOneInnerJoinWithMultiRelatedKeysShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneInnerJoinWithMultiRelatedKeys"));
    }
    
    @Test
    public void testOneToOneRightAndLeftJoinWithMultiRelatedKeysShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("oneToOneLeftJoinWithMultiRelatedKeys"));
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("oneToOneRightJoinWithMultiRelatedKeys"));
    }
    
    @Test
    public void testOneToOneLeftJoinWithGroupByKeysShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("oneToOneLeftJoinWithGroupByKeys"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testOneToManyLeftJoinWithGroupByKeysShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("oneToManyLeftJoinWithGroupByKeys"));
    }
    
    @Test
    public void testOneToManyRightJoinWithGroupByKeysShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-multi-keys.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("oneToManyRightJoinWithGroupByKeys"));
    }
    
    @Test
    public void testManyToOneLeftJoinWithConcatShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneLeftJoinWithConcat"));
    }
    
    @Test
    public void testManyToOneLeftJoinWithConcatWsShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneLeftJoinWithConcatWs"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testManyToOneLeftJoinWithGroupConcatAndJsonObjectShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneLeftJoinWithGroupConcatAndJsonObject"));
    }
    
    @Test
    public void testManyToOneLeftJoinWithGroupConcatAndConcatShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("manyToOneLeftJoinWithGroupConcatAndConcat"));
    }
    
    //@Test
    private void testMinFunctionWithSubqueryShouldInnerTableRetainNeededColumnsShouldAsExpected() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        SqlNode node = wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("minFunctionWithSubquery"));
        SqlSelect outerSqlSelect = (SqlSelect) node;
        Assert.equals(2, outerSqlSelect.getSelectList().size());
        Assert.equals("user_001_id", ((SqlIdentifier) outerSqlSelect.getSelectList().get(0)).names.get(1));
        Assert.equals("min_id", ((SqlIdentifier) outerSqlSelect.getSelectList().get(1)).names.get(1));
        SqlSelect sqlSelect = (SqlSelect) ((SqlBasicCall) outerSqlSelect.getFrom()).getOperandList().get(0);
        SqlSelect innerSelect = (SqlSelect) ((SqlBasicCall) sqlSelect.getFrom()).getOperandList().get(0);
        Assert.equals(2, innerSelect.getSelectList().size());
        Assert.equals("id", ((SqlIdentifier) innerSelect.getSelectList().get(0)).names.get(1));
        Assert.equals("user_001_id", ((SqlIdentifier) innerSelect.getSelectList().get(1)).names.get(1));
    }
    
    @Test
    public void testSelectListWithSameFieldNameShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("basic-single-table.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("selectListWithSameFieldName"));
    }
    
    @Test
    public void testSupportedFunctionInOnConditionShouldPass() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("supportedFunctionInOnCondition"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedFunctionInOnConditionShouldThrowException() throws SqlParseException {
        SchemasAndSqls schemasAndSqls = SqlFileToSchemaAndSqlWithName.getSchemasAndSqls("two-table-join-with-one-key.sql");
        wideTableSqlService.validate(schemasAndSqls.getSchemaTableFieldsMap(), schemasAndSqls.getSqls().get("unsupportedFunctionInOnCondition"));
    }
    
    @Configuration
    @ComponentScan(basePackages = "cn.xdf.acdc.devops.service.process.widetable.sql")
    @EnableAspectJAutoProxy
    static class Config {
    
    }
}
