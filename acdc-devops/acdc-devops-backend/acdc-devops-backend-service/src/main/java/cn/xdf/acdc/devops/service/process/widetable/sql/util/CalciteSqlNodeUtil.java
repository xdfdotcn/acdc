package cn.xdf.acdc.devops.service.process.widetable.sql.util;

import cn.xdf.acdc.devops.service.process.widetable.sql.TableField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class CalciteSqlNodeUtil {
    
    /**
     * Get table fields after `group by`.
     *
     * @param groupByFields fields after GROUP BY
     * @return table field set
     */
    public static Set<TableField> getGroupByTableFields(final SqlNodeList groupByFields) {
        if (groupByFields == null) {
            return new HashSet<>();
        }
        return groupByFields.stream().map(sqlIdentifier -> {
            String table = ((SqlIdentifier) sqlIdentifier).names.get(0);
            String field = ((SqlIdentifier) sqlIdentifier).names.get(1);
            return new TableField(table, field);
        }).collect(Collectors.toSet());
    }
}
