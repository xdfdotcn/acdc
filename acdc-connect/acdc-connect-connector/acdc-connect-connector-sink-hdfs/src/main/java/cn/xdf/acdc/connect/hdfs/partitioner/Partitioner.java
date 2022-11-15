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

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Partition incoming records, and generates directories and file names in which to store the
 * incoming records.
 *
 * @param <T> The type representing the field schemas.
 */
public interface Partitioner<T> {

    /**
     * initialize the config .
     * @param config  config
     */
    void configure(Map<String, Object> config);

    /**
     * Returns string representing the output path for a sinkRecord to be encoded and stored.
     *
     * @param sinkRecord The record to be stored by the Sink Connector
     * @return The path/filename the SinkRecord will be stored into after it is encoded
     */
    String encodePartition(SinkRecord sinkRecord);

    /**
     * Returns string representing the output path for a sinkRecord to be encoded and stored.
     *
     * @param sinkRecord The record to be stored by the Sink Connector
     * @param nowInMillis The current time in ms. Some Partitioners will use this option, but by
     *                    default it is unused.
     * @return The path/filename the SinkRecord will be stored into after it is encoded
     */
    default String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
        return encodePartition(sinkRecord);
    }

    /**
     * generate the partition path .
     * @param tableName  table name
     * @param encodedPartition the partition
     * @return partition path
     */
    String generatePartitionedPath(String tableName, String encodedPartition);

    /**
     * Get partition fields.
     * @return Partition filed list
     * */
    List<T> partitionFields();
}
