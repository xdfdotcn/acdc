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

import cn.xdf.acdc.connect.core.sink.AbstractBufferedRecords;
import cn.xdf.acdc.connect.core.sink.AbstractBufferedWriter;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class HdfsWriter extends AbstractBufferedWriter<HdfsWriterCoordinator> {

    private final HdfsWriterCoordinator hdfsWriterCoordinator;

    private HdfsSinkConfig sinkConfig;

    public HdfsWriter(
        final HdfsSinkConfig sinkConfig,
        final SinkTaskContext context) {
        super(sinkConfig);
        this.sinkConfig = sinkConfig;
        this.hdfsWriterCoordinator = new HdfsWriterCoordinator(sinkConfig, context);
    }

    @Override
    protected HdfsWriterCoordinator getClient() throws ConnectException {
        return hdfsWriterCoordinator;
    }

    @Override
    public void closePartitions(final Collection<TopicPartition> partitions) {

    }

    @Override
    protected AbstractBufferedRecords getBufferedRecords(
        final HdfsWriterCoordinator connection,
        final String tableName) throws ConnectException {
        return new HdfsBufferedRecords(sinkConfig, connection);
    }

    @Override
    protected void commit(final HdfsWriterCoordinator connection) throws ConnectException {
        connection.commit();
    }

    @Override
    public void close() throws ConnectException {
        hdfsWriterCoordinator.close();
    }

    /**
     * Recover all Writer.
     * @param assignment assigned topic partitions
     */
    public void recover(final Set<TopicPartition> assignment) {
        hdfsWriterCoordinator.recover(assignment);
    }

    /**
     * Stop all writer.
     */
    public void stop() {
        hdfsWriterCoordinator.stop();
    }

    /**
     * Open  all Writer.
     * @param partitions assigned topic partitions
     */
    public void open(final Collection<TopicPartition> partitions) {
        hdfsWriterCoordinator.open(partitions);
    }

    /**
     * Get committed offsets.
     * @return offsets off all topic partitions
     */
    public Map<TopicPartition, Long> getCommittedOffsets() {
        return hdfsWriterCoordinator.getCommittedOffsets();
    }
}
