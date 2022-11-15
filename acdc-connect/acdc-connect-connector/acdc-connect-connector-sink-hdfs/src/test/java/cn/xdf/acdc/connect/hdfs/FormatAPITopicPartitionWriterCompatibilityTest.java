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

import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroDataFileReader;
import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.writer.ExactlyOnceTopicPartitionWriter;
import cn.xdf.acdc.connect.hdfs.writer.TopicPartitionWriter;
import io.confluent.common.utils.MockTime;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test to ensure we can still instantiate & use the old-style HDFS-only interfaces instead of
 * those from storage-common and use them with TopicPartitionWriter.
 */
public class FormatAPITopicPartitionWriterCompatibilityTest extends TestWithMiniDFSCluster {

    private RecordWriterProvider writerProvider;

    private RecordWriterProvider newWriterProvider;

    private HdfsStorage storage;

    private MockTime time;

    @Override
    protected Map<String, String> createProps() {
        return super.createProps();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        time = new MockTime();
        storage = new HdfsStorage(connectorConfig, url);
        writerProvider = defaultStoreContext.getRecordWriterProvider();
        newWriterProvider = null;
        dataFileReader = new AvroDataFileReader();
        extension = writerProvider.getExtension();

        createTablePath(defaultStoreContext.getStoreConfig().tablePath());
        createLogsDir(defaultStoreContext.getStoreConfig().walLogPath());
    }

    @Test
    public void testWriteRecordDefaultWithPadding() throws Exception {
        Partitioner partitioner = new DefaultPartitioner();
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);
        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
            context,
            time,
            TOPIC_PARTITION,
            defaultStoreContext
        );

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 3);
        // Add a single records at the end of the batches sequence. Total records: 10
        records.add(createRecord(schema));
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.recover();
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        // No verification since the format is a dummy format. We're really just trying to exercise
        // the old APIs and any paths that should hit them (and not NPE due to the variables for
        // new-style formats being null)
    }

    private void createTablePath(final String tablePath) throws IOException {
        Path path = new Path(tablePath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }

    private void createLogsDir(final String logsDir) throws IOException {
        Path path = new Path(logsDir);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }
}
