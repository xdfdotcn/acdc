package cn.xdf.acdc.devops.core.util;

import com.google.common.base.Preconditions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 时间Util.
 */
public class DateUtil {
    
    private static final DateTimeFormatter YTD_HOUR_MINUTE_SECOND = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 时间格式化.
     *
     * @param instant instant
     * @return 格式化后的时间字符串
     */
    public static String formatToString(final Instant instant) {
        Preconditions.checkNotNull(instant);
        return ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()).format(YTD_HOUR_MINUTE_SECOND);
    }
    
    /**
     * 时间字符串解析: yyyy-MM-dd HH:mm:ss.
     *
     * @param text text
     * @return 解析后的时间戳
     */
    public static Instant parseToInstant(final String text) {
        ZonedDateTime zonedDateTime = ZonedDateTime.of(
                LocalDateTime.parse(text, YTD_HOUR_MINUTE_SECOND),
                ZoneId.systemDefault()
        );
        return zonedDateTime.toInstant();
    }
    
    /**
     * Get Instant.
     *
     * @param instant instant
     * @param supplier lambda
     * @return instant
     */
    public static Instant getInstantWithDefault(final Instant instant, final Supplier<Instant> supplier) {
        if (Objects.isNull(instant)) {
            return supplier.get();
        }
        return instant;
    }
}
