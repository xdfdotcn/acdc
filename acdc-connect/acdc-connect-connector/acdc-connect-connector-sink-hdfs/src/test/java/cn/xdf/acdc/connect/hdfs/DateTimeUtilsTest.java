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

package cn.xdf.acdc.connect.hdfs;

import cn.xdf.acdc.connect.hdfs.util.DateTimeUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DateTimeUtilsTest {

    private final DateTime midnight = DateTime.now().withTimeAtStartOfDay();

    private DateTime calc(final DateTime current, long periodMs) {
        return new DateTime(DateTimeUtils.getNextTimeAdjustedByDay(
            current.getMillis(),
            periodMs,
            current.getZone())
        );
    }

    private DateTime calcHourPeriod(final DateTime current) {
        return calc(current, 1000 * 60 * 60);
    }

    @Test
    public void testGetNextTimeAdjustedByDayWOTimeZone() {
        assertEquals(calcHourPeriod(midnight), midnight.plusHours(1));
        assertEquals(calcHourPeriod(midnight.minusSeconds(1)), midnight);
        assertEquals(calcHourPeriod(midnight.plusSeconds(1)), midnight.plusHours(1));
        assertEquals(calcHourPeriod(midnight.plusHours(1)), midnight.plusHours(2));
        assertEquals(calcHourPeriod(midnight.plusHours(1).minusSeconds(1)), midnight.plusHours(1));
    }

    @Test
    public void testGetNextTimeAdjustedByDayPeriodDoesNotFitIntoDay() {
        DateTime midnight = DateTime.now().withTimeAtStartOfDay();
        long sevenHoursMs = 7 * 60 * 60 * 1000;
        assertEquals(calc(midnight, sevenHoursMs), midnight.plusHours(7));
        assertEquals(calc(midnight.plusSeconds(1), sevenHoursMs), midnight.plusHours(7));
        assertEquals(calc(midnight.plusSeconds(1), sevenHoursMs), midnight.plusHours(7));
        assertEquals(calc(midnight.minusSeconds(1), sevenHoursMs), midnight);
        assertEquals(calc(midnight.minusHours(7).minusSeconds(1), sevenHoursMs), midnight.minusDays(1).plusHours(21));
    }
}
