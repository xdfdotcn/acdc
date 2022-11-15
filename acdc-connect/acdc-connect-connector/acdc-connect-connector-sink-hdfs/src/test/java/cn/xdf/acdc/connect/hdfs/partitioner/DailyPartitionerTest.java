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

package cn.xdf.acdc.connect.hdfs.partitioner;

import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DailyPartitionerTest extends TestWithMiniDFSCluster {

    private static final long PARTITION_DURATION_MS = TimeUnit.HOURS.toMillis(24);

    @Test
    public void testDailyPartitioner() throws Exception {
        setUp();
        DailyPartitioner partitioner = new DailyPartitioner();
        partitioner.configure(parsedConfig);

        String pathFormat = partitioner.getPathFormat();
        String timeZoneString = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
        long timestamp = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String encodedPartition = TimeUtils.encodeTimestamp(PARTITION_DURATION_MS, pathFormat, timeZoneString, timestamp);
        String path = partitioner.generatePartitionedPath("topic", encodedPartition);
        assertEquals("topic/year=2014/month=02/day=01", path);
    }
}
