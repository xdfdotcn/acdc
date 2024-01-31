package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import cn.xdf.acdc.devops.service.process.widetable.sql.transformer.SubQueryReplacementTransformer;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

public class SubQueryReplacementTransformerTest {
    
    private static final SqlFileReader READER = new SqlFileReader("sub-query-replacement-transformer-test");
    
    @Test(expected = AcdcServiceException.class)
    public void testNonQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQueryWithOrderBy() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithOrderBy");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testNonJoinQueryWhenFromWithoutSelect() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonJoinQueryWhenFromWithoutSelect");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testNonJoinQueryWhenFromWithJoinSelect() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonJoinQueryWhenFromWithJoinSelect");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testNonJoinQueryWhenFromWithSingleTableSbuQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonJoinQueryWhenFromWithSingleTableSbuQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQueryWithSingleTableSubQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithSingleTableSubQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    //    @Test
    //    public void testJoinQueryWithJoinSubQuery() {
    //        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithJoinSubQuery");
    //        String originalSql = testCaseSql.getOriginalSql();
    //        String optimizedSql = testCaseSql.getOptimizedSql();
    //        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
    //        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
    //        SqlNode newSqlNode = transformer.transform(sqlNode, null);
    //
    //        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    //    }
    
    @Test
    public void testJoinQueryWithSchema() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithSchema");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        SubQueryReplacementTransformer transformer = new SubQueryReplacementTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        System.out.println(newSqlNode);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
}
