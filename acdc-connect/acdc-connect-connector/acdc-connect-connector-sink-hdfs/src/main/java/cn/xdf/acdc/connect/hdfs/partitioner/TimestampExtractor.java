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

import java.util.Map;
import org.apache.kafka.connect.connector.ConnectRecord;

public interface TimestampExtractor {

    /**
     initialize the config .
     @param config  config
     */
    void configure(Map<String, Object> config);

    /**
     * Extract timestamp from a record.
     *
     * @param record Record from which to extract a timestamp
     * @return Timestamp in milliseconds
     */
    Long extract(ConnectRecord<?> record);

    /**
     * Extract timestamp from a record.
     *
     * @param record Record from which to extract a timestamp
     * @param nowInMillis Current time in milliseconds an implementation may use or return
     * @return Timestamp in milliseconds
     */
    default Long extract(ConnectRecord<?> record, long nowInMillis) {
        return extract(record);
    }
}
