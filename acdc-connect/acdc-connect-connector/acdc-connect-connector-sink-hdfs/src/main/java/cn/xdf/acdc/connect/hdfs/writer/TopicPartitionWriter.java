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

package cn.xdf.acdc.connect.hdfs.writer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public interface TopicPartitionWriter {

    /**
     * Buffering record with offset.
     *
     * @param sinkRecord kafka message record
     */
    void buffer(SinkRecord sinkRecord);

    /**
     * Write to Hdfs.
     */
    void write();

    /**
     * Gets the processing completed offset.
     *
     * @return completed offset
     */
    long offset();

    /**
     * Gets TopicPartition.
     *
     * @return TopicPartition
     */
    TopicPartition topicPartition();

    /**
     * Fault recovery.
     *
     * @return Is Successful
     */
    boolean recover();

    /**
     * Writer close.
     */
    void close();

    /**
     * Commit file  and  close writers.
     */
    void commit();

    /**
     * Change schema.
     * @param curSchema The current newest schema
     * @return After processing the schema
     */
    Schema doChangeSchema(Schema curSchema);
}
