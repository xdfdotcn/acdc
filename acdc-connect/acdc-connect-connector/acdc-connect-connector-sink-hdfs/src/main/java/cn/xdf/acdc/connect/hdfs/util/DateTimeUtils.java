/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs.util;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import org.apache.kafka.connect.errors.ConnectException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

public class DateTimeUtils {

    /**
     * Calculates next period of periodMs after currentTimeMs
     * starting from midnight in given timeZone.
     * If the next period is in next day then 12am of next day
     * will be returned
     *
     * @param currentTimeMs time to calculate at
     * @param periodMs period in ms
     * @param timeZone timezone to get midnight time
     * @return timestamp in ms
     */
    public static long getNextTimeAdjustedByDay(
        long currentTimeMs,
        long periodMs,
        final DateTimeZone timeZone
    ) {
        DateTime currentDT = new DateTime(currentTimeMs).withZone(timeZone);
        DateTime startOfDayDT = currentDT.withTimeAtStartOfDay();
        DateTime startOfNextDayDT = startOfDayDT.plusDays(1);
        Duration currentDayDuration = new Duration(startOfDayDT, startOfNextDayDT);
        long todayInMs = currentDayDuration.getMillis();

        long startOfDay = startOfDayDT.getMillis();
        long nextPeriodOffset = ((currentTimeMs - startOfDay) / periodMs + 1) * periodMs;
        long offset = Math.min(nextPeriodOffset, todayInMs);
        return startOfDay + offset;
    }

    /**
     * Fix time when not specified time zone,minus time lag.
     * @param date without time zone date
     * @return the fixed date
     */
    public static java.util.Date fixTimeZone(final java.util.Date date) {
        return new java.util.Date(date.getTime() - TimeZone.getDefault().getRawOffset());
    }

    /**
     * Verify date and parse date for timestamp string with time zone.
     *
     * @param zonedTimestamp with time zone timestamp string
     * @return the parsed date
     */
    public static java.util.Date verifyDateFormatAndGetDate(final String zonedTimestamp) {
        try {
            return ZonedTimestamp.parseToDate(zonedTimestamp);
        } catch (DateTimeParseException e) {
            throw new ConnectException(e);
        }
    }
}
