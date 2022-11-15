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

import cn.xdf.acdc.connect.hdfs.writer.HdfsWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class HdfsSinkTask extends SinkTask {

    private HdfsWriter hdfsWriter;

    private String taskId;

    private String connectorName;

    private String connectorNameAndTaskId;

    public HdfsSinkTask() {
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        connectorName = props.get(HdfsSinkConnector.CONNECTOR_CONFIG_NAME);
        taskId = props.get(HdfsSinkConnector.TASK_ID_CONFIG_NAME);
        connectorNameAndTaskId = String.format("%s-%s", connectorName, taskId);
        log.info("Starting HDFS Sink Task {}", connectorNameAndTaskId);

        try {
            HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
            this.hdfsWriter = new HdfsWriter(connectorConfig, context);
            recover(context.assignment());
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start HdfsSinkConnector due to configuration error.", e);
        } catch (ConnectException e) {
            log.error("Couldn't start HdfsSinkConnector:", e);
            log.info("Shutting down HdfsSinkConnector.");
            if (hdfsWriter != null) {
                try {
                    log.debug("Closing data writer due to task start failure.");
                    hdfsWriter.close();
                } finally {
                    log.debug("Stopping data writer due to task start failure.");
                    hdfsWriter.stop();
                }
            }
            // Always throw the original exception that prevent us from starting
            throw e;
        }

        log.info("The connector relies on offsets in the WAL files, if these are not present it uses "
            + "the filenames in HDFS. In both cases the connector commits offsets to Connect to "
            + "enable monitoring progress of the HDFS connector. Upon startup, the HDFS "
            + "Connector restores offsets from the WAL log files, if these are not present it "
            + "uses the filenames in HDFS. In the absence of files in HDFS, "
            + "the connector will attempt to find offsets for its consumer group in the "
            + "'__consumer_offsets' topic. If offsets are not found, the consumer will "
            + "rely on the reset policy specified in the 'consumer.auto.offset.reset' property to "
            + "start exporting data to HDFS.");
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        log.debug("Read {} records from Kafka", records.size());
        try {
            hdfsWriter.write(records);
        } catch (ConnectException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets
    ) {
        // Although the connector manages offsets via files in HDFS, we still want to have Connect
        // commit the consumer offsets for records this task has consumed from its topic partitions and
        // committed to HDFS.
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : hdfsWriter.getCommittedOffsets().entrySet()) {
            log.debug(
                "Found last committed offset {} for topic partition {}",
                entry.getValue(),
                entry.getKey()
            );
            result.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }
        log.debug("Returning committed offsets {}", result);
        return result;
    }

    @Override
    public void open(final Collection<TopicPartition> partitions) {
        log.info("Opening HDFS Sink Task {}", connectorNameAndTaskId);
        hdfsWriter.open(partitions);
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        log.info("Closing HDFS Sink Task {}", connectorNameAndTaskId);
        if (hdfsWriter != null) {
            hdfsWriter.close();
        }
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Stopping HDFS Sink Task {}", connectorNameAndTaskId);
        if (hdfsWriter != null) {
            hdfsWriter.close();
            hdfsWriter.stop();
        }
    }

    private void recover(final Set<TopicPartition> assignment) {
        hdfsWriter.recover(assignment);
    }
}
