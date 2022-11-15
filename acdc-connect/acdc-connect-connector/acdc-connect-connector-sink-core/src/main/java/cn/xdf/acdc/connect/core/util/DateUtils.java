package cn.xdf.acdc.connect.core.util;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtils {

    public static final DateTimeFormatter DEFAULT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final ZoneId ZONE_UTC = ZoneId.of("UTC");

    /**
     * Parse the date to string in format yyyy-MM-dd HH:mm:ss.
     *
     * @param date data to parse
     * @param zoneId zone id
     * @return parse result in string
     */
    public static String format(final Date date, final ZoneId zoneId) {
        return DEFAULT_FORMAT.withZone(zoneId).format(date.toInstant());
    }

    /**
     * Parse the date to string in format ISO_OFFSET_DATE_TIME,such as '2011-12-03T10:15:30+01:00'.
     *
     * @param date data to parse
     * @param zoneId zone id
     * @return parse result in string
     */
    public static String formatWithZone(final Date date, final ZoneId zoneId) {
        return ZonedDateTime.ofInstant(date.toInstant(), zoneId).format(ZonedTimestamp.FORMATTER);
    }
}
