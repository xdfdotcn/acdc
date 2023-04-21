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

package cn.xdf.acdc.connect.hdfs.format;

import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;

import java.io.Closeable;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface for reading a schema from the storage.
 */
public interface SchemaReader extends Closeable {

    /**
     * Project record.
     *
     * @param topicPartition Kafka topic partition.
     * @param sinkRecord     Kafka connect sinkRecord.
     * @return Projected record
     */
    ProjectedResult projectRecord(TopicPartition topicPartition, SinkRecord sinkRecord);

    /**
     * Get HiveFactory.
     *
     * @return The hive util factory
     */
    HiveFactory getHiveFactory();

    /**
     * Get table status.
     *
     * @param topicPartition topic partition
     * @return The hive table info ,schema and hdfs partition dir list
     */
    TableSchemaAndDataStatus getTableSchemaAndDataStatus(TopicPartition topicPartition);
}
