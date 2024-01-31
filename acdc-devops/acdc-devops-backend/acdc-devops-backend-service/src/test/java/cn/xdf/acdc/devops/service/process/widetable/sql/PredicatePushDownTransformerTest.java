package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.widetable.sql.transformer.PredicatePushDownTransformer;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// CHECKSTYLE:OFF
public class PredicatePushDownTransformerTest {
    @Test
    public void testJoinedSubQuery() {
        String originalSql = "select u.id,o.id from users u inner join "
                + "(select oo.id,oo.user_id,oo.price,oo.goods from orders oo inner join users uu on oo.user_id=uu.id and uu.id<100 and oo.id>100) o "
                + "on u.id=o.user_id and u.name='frank' and o.goods='apple' where u.age>10 and o.price >1000";
        String optimizedSql = "SELECT `t0`.`id`, `t7`.`id` AS `id0`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`age` > 10 AND `users`.`name` = 'frank') AS `t0`\n" +
                "INNER JOIN (SELECT `t4`.`id`, `t4`.`user_id`, `t4`.`price`, `t4`.`goods`\n" +
                "FROM (SELECT `t1`.`id`, `t1`.`user_id`, `t1`.`name`, `t1`.`goods`, `t1`.`price`\n" +
                "FROM (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE CAST(`orders`.`id` AS INTEGER) > 100) AS `t1`\n" +
                "WHERE `t1`.`price` > 1000 AND `t1`.`goods` = CAST('apple' AS INTEGER)) AS `t4`\n" +
                "INNER JOIN (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE CAST(`users`.`id` AS INTEGER) < 100) AS `t6` ON `t4`.`user_id` = `t6`.`id`) AS `t7` ON `t0`.`id` = `t7`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testOnConditionForLeftJoinWhenOnlyContainLeftPredicate() {
        String originalSql = "select users.id from users left join orders on users.id = orders.user_id and users.age >100";
        String optimizedSql = "SELECT `users`.`id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "LEFT JOIN `s1`.`orders` AS `orders` ON `users`.`id` = `orders`.`user_id` AND `users`.`age` > 100\n";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testOnConditionForLeftJoinWhenOnlyContainRightPredicate() {
        String originalSql = "select users.id from users left join orders on users.id = orders.user_id and orders.price >100";
        String optimizedSql = "SELECT `users`.`id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "LEFT JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`price` > 100) AS `t0` ON `users`.`id` = `t0`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testOnConditionForLeftJoinWhenContainRightAndLeftPredicate() {
        // 会把 left join 转换成 inner join 所有谓词都可以下推
        String originalSql = "select users.id from users left join orders on users.id = orders.user_id and users.age >100 and orders.price>1000";
        String optimizedSql = "SELECT `users`.`id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "LEFT JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`price` > 1000) AS `t0` ON `users`.`id` = `t0`.`user_id` AND `users`.`age` > 100";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testOnConditionForInnerJoin() {
        // inner join on 和 where 中的所有谓词都能下推
        String originalSql = "select users.id from users inner join orders on users.id = orders.user_id and orders.price >100 and users.age >10";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`age` > 10) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`price` > 100) AS `t2` ON `t0`.`id` = `t2`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testWhereConditionForLeftJoinWhenOnlyContainLeftPredicate() {
        String originalSql = "select users.id from users left join orders on users.id = orders.user_id and users.age >100 and orders.price >1000 where users.name='frank'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`name` = 'frank') AS `t0`\n" +
                "LEFT JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`price` > 1000) AS `t2` ON `t0`.`id` = `t2`.`user_id` AND `t0`.`age` > 100\n";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testWhereConditionForLeftJoinWhenOnlyContainRightPredicate() {
        String originalSql = "select users.id from users left join orders on users.id = orders.user_id and users.age >100 and orders.price >1000 where orders.goods='apple'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`age` > 100) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`goods` = CAST('apple' AS INTEGER) AND `orders`.`price` > 1000) AS `t2` ON `t0`.`id` = `t2`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testWhereConditionForLeftJoinWhenContainRightAndLeftPredicate() {
        String originalSql = "select users.id from users left join orders "
                + "on users.id = orders.user_id and users.age >100 and orders.price >1000 where orders.goods='apple' and users.name='frank'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`name` = 'frank' AND `users`.`age` > 100) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`goods` = CAST('apple' AS INTEGER) AND `orders`.`price` > 1000) AS `t2` ON `t0`.`id` = `t2`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testWhereConditionForInnerJoin() {
        // inner join on 和 where 中的所有谓词都能下推
        String originalSql = "select users.id from users inner join orders "
                + "on users.id = orders.user_id and users.age >100 and orders.price >1000 where orders.goods = 'apple' and users.name='frank'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`name` = 'frank' AND `users`.`age` > 100) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`goods` = CAST('apple' AS INTEGER) AND `orders`.`price` > 1000) AS `t2` ON `t0`.`id` = `t2`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testFunctionPredicateForSingleCol() {
        String originalSql = "select users.id from users inner join orders "
                + "on users.id = orders.user_id and users.age >100 and orders.price >1000 where SUBSTRING(users.name,3)='frank' and orders.goods = 'apple'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE SUBSTRING(`users`.`name`, 3) = 'frank' AND `users`.`age` > 100) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`goods` = CAST('apple' AS INTEGER) AND `orders`.`price` > 1000) AS `t2` ON `t0`.`id` = `t2`.`user_id`";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testFunctionPredicateForMultiDifferentTableCol() {
        String originalSql = "select users.id from users inner join orders "
                + "on users.id = orders.user_id and users.age >100 and orders.price >1000 where CONCAT(users.name,'-',orders.goods)='frank-apple' and orders.goods = 'apple'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE `users`.`age` > 100) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`goods` = CAST('apple' AS INTEGER) AND `orders`.`price` > 1000) AS `t2` "
                + "ON `t0`.`id` = `t2`.`user_id` AND CONCAT(`t0`.`name`, '-', CAST(`t2`.`goods` AS CHAR(1))) = 'frank-apple'";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    @Test
    public void testFunctionPredicateForMultiSameTableCol() {
        String originalSql = "select users.id from users inner join orders "
                + "on users.id = orders.user_id and users.age >100 and orders.price >1000 where CONCAT(users.name,'-',users.age)='frank-10' and orders.goods = 'apple'";
        String optimizedSql = "SELECT `t0`.`id`\n" +
                "FROM (SELECT `users`.`id`, `users`.`name`, `users`.`age`, `users`.`parent_id`\n" +
                "FROM `s1`.`users` AS `users`\n" +
                "WHERE CONCAT(`users`.`name`, '-', CAST(`users`.`age` AS CHAR(1))) = 'frank-10' AND `users`.`age` > 100) AS `t0`\n" +
                "INNER JOIN (SELECT `orders`.`id`, `orders`.`user_id`, `orders`.`name`, `orders`.`goods`, `orders`.`price`\n" +
                "FROM `s1`.`orders` AS `orders`\n" +
                "WHERE `orders`.`goods` = CAST('apple' AS INTEGER) AND `orders`.`price` > 1000) AS `t2` ON `t0`.`id` = `t2`.`user_id`\n";
        SqlNode sqlNode = MySqlSqlParser.parse(originalSql);
        
        PredicatePushDownTransformer transformer = new PredicatePushDownTransformer();
        SqlNode newSqlNode = transformer.transform(sqlNode, getSchemaInfo());
        
        Assert.assertTrue(SqlUtil.equals(newSqlNode.toString(), optimizedSql));
    }
    
    private Map<String, Map<String, List<DataFieldDefinition>>> getSchemaInfo() {
        Map<String, Map<String, List<DataFieldDefinition>>> schema = new HashMap<>();
        Map<String, List<DataFieldDefinition>> table = new HashMap<>();
        table.put("users", Lists.newArrayList(
                new DataFieldDefinition("id", "varchar(100)", Schema.STRING_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("name", "varchar(100)", Schema.STRING_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("age", "int", Schema.INT32_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("parent_id", "varchar(100)", Schema.STRING_SCHEMA, false, "", null, new HashSet<>())
        ));
        table.put("orders", Lists.newArrayList(
                new DataFieldDefinition("id", "varchar(100)", Schema.STRING_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("user_id", "varchar(100)", Schema.STRING_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("name", "varchar(100)", Schema.STRING_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("goods", "int", Schema.INT32_SCHEMA, false, "", null, new HashSet<>()),
                new DataFieldDefinition("price", "int", Schema.INT32_SCHEMA, false, "", null, new HashSet<>())
        ));
        schema.put("s1", table);
        return schema;
    }
}
