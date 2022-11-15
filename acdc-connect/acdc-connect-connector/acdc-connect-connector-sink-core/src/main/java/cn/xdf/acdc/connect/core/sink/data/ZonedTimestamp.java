
package cn.xdf.acdc.connect.core.sink.data;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public final class ZonedTimestamp {

    /**
     * The ISO date-time format includes the date, time (including fractional parts), and offset from UTC, such as
     * '2011-12-03T10:15:30.030431+01:00'.
     */
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static final String LOGICAL_NAME = "io.debezium.time.ZonedTimestamp";

    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private ZonedTimestamp() {
    }

    /**
     * Parse a timestamp string to date.
     *
     * @param zonedTimestamp a timestamp string
     * @return date
     */
    public static Date parseToDate(final String zonedTimestamp) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(zonedTimestamp, FORMATTER);
        return Date.from(zonedDateTime.toInstant());
    }

}
