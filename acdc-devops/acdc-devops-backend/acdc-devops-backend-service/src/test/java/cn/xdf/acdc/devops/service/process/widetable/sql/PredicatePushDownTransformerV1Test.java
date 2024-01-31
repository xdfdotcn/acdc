package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import cn.xdf.acdc.devops.service.process.widetable.sql.transformer.PredicatePushDownTransformerV1;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

// CHECKSTYLE:OFF
public class PredicatePushDownTransformerV1Test {
    
    private static final SqlFileReader READER = new SqlFileReader("predicate-push-down-transformer-test");
    
    @Test(expected = AcdcServiceException.class)
    public void testNonQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test(expected = AcdcServiceException.class)
    public void testSingleTableQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testSingleTableQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test(expected = AcdcServiceException.class)
    public void testJoinQueryWithoutSingleTableSubQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithoutSingleTableSubQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQueryWithNonPredicate() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithNonPredicate");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQueryWithLiteralPredicate() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithLiteralPredicate");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQueryWithIdentifierPredicate() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithIdentifierPredicate");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test(expected = AcdcServiceException.class)
    public void testJoinQueryWithOrPredicate() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithOrPredicate");
        String originalSql = testCaseSql.getOriginalSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        transformer.transform(sqlNode, null);
    }
    
    @Test
    public void testJoinQueryWithJoinSubQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithJoinSubQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testJoinQueryWithHaving() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testJoinQueryWithHaving");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testNonJoinQueryWhenFromWithSingleTableSbuQuery() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonJoinQueryWhenFromWithSingleTableSbuQuery");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test
    public void testNonJoinQueryWhenFromWithJoinSelect() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonJoinQueryWhenFromWithJoinSelect");
        String originalSql = testCaseSql.getOriginalSql();
        String optimizedSql = testCaseSql.getOptimizedSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        SqlNode newSqlNode = transformer.transform(sqlNode, null);
        
        Assert.assertTrue(SqlUtil.equals(optimizedSql, newSqlNode.toString()));
    }
    
    @Test(expected = AcdcServiceException.class)
    public void testNonJoinQueryWhenFromWithoutSelect() {
        SqlFileReader.TestCaseSql testCaseSql = READER.readTestCase("testNonJoinQueryWhenFromWithoutSelect");
        String originalSql = testCaseSql.getOriginalSql();
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        PredicatePushDownTransformerV1 transformer = new PredicatePushDownTransformerV1();
        transformer.transform(sqlNode, null);
    }
}
