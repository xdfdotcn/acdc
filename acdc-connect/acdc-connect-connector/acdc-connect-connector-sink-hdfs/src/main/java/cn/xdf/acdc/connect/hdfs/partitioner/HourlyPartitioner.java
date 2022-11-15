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

import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HourlyPartitioner<T> extends TimeBasedPartitioner<T> {

    @Override
    public void configure(final Map<String, Object> config) {
        long partitionDurationMs = TimeUnit.HOURS.toMillis(1);

        String delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
        String pathFormat =
            "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd" + delim + "'hour'=HH";

        config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, partitionDurationMs);
        config.put(PartitionerConfig.PATH_FORMAT_CONFIG, pathFormat);
        super.configure(config);
    }
}
