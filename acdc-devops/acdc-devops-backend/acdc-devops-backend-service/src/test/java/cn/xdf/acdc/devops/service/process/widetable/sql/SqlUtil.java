package cn.xdf.acdc.devops.service.process.widetable.sql;

import java.util.Arrays;
import java.util.Objects;

public class SqlUtil {
    
    /**
     * 判断 SQL 语句是否相等，忽略大小写，和空字符.
     *
     * @param sql1 sql
     * @param sql2 sql
     * @return bool
     */
    public static boolean equals(final String sql1, final String sql2) {
        if (Objects.isNull(sql1) && Objects.isNull(sql2)) {
            return true;
        }
        if (Objects.isNull(sql1) || Objects.isNull(sql2)) {
            return false;
        }
        String newSql1 = format(sql1);
        String newSql2 = format(sql2);
        return newSql1.equals(newSql2);
    }
    
    /**
     * Sql 格式化，压缩.
     *
     * @param sql sql
     * @return 格式化压缩后的 sql
     */
    public static String format(final String sql) {
        String newSql = sql.replaceAll("\r|\n|\\s|`", "").toLowerCase();
        char[] chars = newSql.toCharArray();
        Arrays.sort(chars);
        newSql = new String(chars);
        return newSql;
    }
}
