package cn.xdf.acdc.devops.core.util;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Objects;

/**
 * DB 查询工具类.
 */
public class QueryUtil {
    
    /**
     * like 条件拼接.
     *
     * @param values 拼接字符
     * @return 拼接字符串
     */
    public static String like(final String... values) {
        Preconditions.checkArgument(Objects.nonNull(values) && values.length > 0);
        
        StringBuilder builder = new StringBuilder();
        Arrays.stream(values).forEach(s -> builder.append(s));
        return builder.toString();
    }
    
    /**
     * 判断ID是否为空.
     *
     * @param id id
     * @return boolean
     */
    public static boolean isNullId(final Long id) {
        return Objects.isNull(id) || id <= 0;
    }
}
