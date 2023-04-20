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

import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroDataFileReader;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import io.confluent.connect.avro.AvroData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsSinkTaskTest extends TestWithMiniDFSCluster {

    private static final String DIRECTORY1 = "partition=" + String.valueOf(PARTITION);

    private static final String DIRECTORY2 = "partition=" + String.valueOf(PARTITION2);

    private static final String EXTENSION = ".avro";

    private final DataFileReader schemaFileReader = new AvroDataFileReader();

    @Test
    public void testSinkTaskStart() throws Exception {
        setUp();
        createCommittedFiles();
        HdfsSinkTask task = new HdfsSinkTask();

        task.initialize(context);
        task.start(properties);

        Map<TopicPartition, Long> offsets = context.offsets();
        assertEquals(offsets.size(), 2);
        assertTrue(offsets.containsKey(TOPIC_PARTITION));
        assertEquals(21, (long) offsets.get(TOPIC_PARTITION));
        assertTrue(offsets.containsKey(TOPIC_PARTITION2));
        assertEquals(46, (long) offsets.get(TOPIC_PARTITION2));

        task.stop();
    }

    @Test
    public void testSinkTaskFileSystemIsolation() throws Exception {
        // Shutdown of one task should not affect another task
        setUp();
        createCommittedFiles();

        // Generate two rounds of data two write at separate times
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        SinkRecord mockRecord =
                new SinkRecord(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), Schema.STRING_SCHEMA, key, schema, record,
                        0);
        SinkRecord processRecord = processorProvider.getProcessor(StoreConstants.TABLES).process(mockRecord);
        Schema processSchema = processRecord.valueSchema();
        Collection<SinkRecord> sinkRecordsA = new ArrayList<>();
        Collection<SinkRecord> sinkRecordsB = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record,
                                offset);
                sinkRecordsA.add(sinkRecord);
            }
            for (long offset = 7; offset < 16; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record,
                                offset);
                sinkRecordsB.add(sinkRecord);
            }
        }

        HdfsSinkTask task = new HdfsSinkTask();
        task.initialize(context);
        task.start(properties);
        task.put(sinkRecordsA);

        // Get an aliased reference to the filesystem object from the per-worker FileSystem.CACHE
        // Close it to induce exceptions when aliased FileSystem objects are used after closing.
        // Paths within this filesystem (such as the WAL) will also share the same FileSystem object
        // because the cache is keyed on uri.getScheme() and uri.getAuthority().
        FileSystem.get(
                new URI(connectorConfig.getString(HdfsSinkConfig.HDFS_URL_CONFIG)),
                connectorConfig.getHadoopConfiguration()
        ).close();

        // If any FileSystem-based resources are kept in-use between put calls, they should generate
        // exceptions on a subsequent put. These exceptions must not affect the correctness of the task.
        task.put(sinkRecordsB);
        task.stop();

        // Verify that the data arrived correctly
//    AvroData avroData = task.getAvroData();
        avroData = new AvroData(connectorConfig.avroDataConfig());
        // Last file (offset 15) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = {-1, 2, 5, 8, 11, 14};

        for (TopicPartition tp : context.assignment()) {
            String partition = "partition=" + String.valueOf(tp.partition());
            for (int j = 1; j < validOffsets.length; ++j) {
                long startOffset = validOffsets[j - 1] + 1;
                long endOffset = validOffsets[j];
                Path path = new Path(
                        FileUtils.jointPath(
                                defaultStoreContext.getStoreConfig().tablePath(),
                                partition,
                                defaultStoreContext.getFileOperator().generateCommittedFileName(tp, startOffset, endOffset, EXTENSION))
                );
                Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
                long size = endOffset - startOffset + 1;
                assertEquals(records.size(), size);
                for (Object avroRecord : records) {
                    assertEquals(avroRecord, avroData.fromConnectData(processSchema, processRecord.value()));
                }
            }
        }
    }

    @Test
    public void testSinkTaskStartNoCommittedFiles() throws Exception {
        setUp();
        HdfsSinkTask task = new HdfsSinkTask();

        task.initialize(context);
        task.start(properties);

        // Without any files in HDFS, we expect no offsets to be set by the connector.
        // Thus, the consumer will start where it last left off or based upon the
        // 'auto.offset.reset' setting, which Connect defaults to 'earliest'.
        Map<TopicPartition, Long> offsets = context.offsets();
        assertEquals(0, offsets.size());

        task.stop();
    }

    @Test
    public void testSinkTaskStartSomeCommittedFiles() throws Exception {
        setUp();
        Map<TopicPartition, List<String>> tempfiles = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateTempFileName(EXTENSION))
        );
        list1.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateTempFileName(EXTENSION))
        );
        tempfiles.put(TOPIC_PARTITION, list1);

        Map<TopicPartition, List<String>> committedFiles = new HashMap<>();
        List<String> list3 = new ArrayList<>();
        list3.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 100, 200, EXTENSION))
        );
        list3.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 201, 300, EXTENSION))

        );
        committedFiles.put(TOPIC_PARTITION, list3);

        for (TopicPartition tp : tempfiles.keySet()) {
            for (String file : tempfiles.get(tp)) {
                fs.createNewFile(new Path(file));
            }
        }

        createWALs(tempfiles, committedFiles);
        HdfsSinkTask task = new HdfsSinkTask();

        task.initialize(context);
        task.start(properties);

        // Without any files in HDFS, we expect no offsets to be set by the connector.
        // Thus, the consumer will start where it last left off or based upon the
        // 'auto.offset.reset' setting, which Connect defaults to 'earliest'.
        Map<TopicPartition, Long> offsets = context.offsets();
        assertEquals(1, offsets.size());
        assertTrue(offsets.containsKey(TOPIC_PARTITION));
        assertEquals(301, (long) offsets.get(TOPIC_PARTITION));

        task.stop();
    }

    @Test
    public void testSinkTaskStartWithRecovery() throws Exception {
        setUp();
        Map<TopicPartition, List<String>> tempfiles = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateTempFileName(EXTENSION))
        );
        list1.add(FileUtils.jointPath(
                defaultStoreContext.getStoreConfig().tablePath(),
                DIRECTORY1,
                defaultStoreContext.getFileOperator().generateTempFileName(EXTENSION))
        );
        tempfiles.put(TOPIC_PARTITION, list1);

        List<String> list2 = new ArrayList<>();
        list2.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY2,
                        defaultStoreContext.getFileOperator().generateTempFileName(EXTENSION))
        );
        list2.add(FileUtils.jointPath(
                defaultStoreContext.getStoreConfig().tablePath(),
                DIRECTORY2,
                defaultStoreContext.getFileOperator().generateTempFileName(EXTENSION))
        );

        tempfiles.put(TOPIC_PARTITION2, list2);

        Map<TopicPartition, List<String>> committedFiles = new HashMap<>();
        List<String> list3 = new ArrayList<>();
        list3.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 100, 200, EXTENSION))
        );
        list3.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 201, 300, EXTENSION))
        );
        committedFiles.put(TOPIC_PARTITION, list3);

        List<String> list4 = new ArrayList<>();
        list4.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY2,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION2, 400, 500, EXTENSION)));
        list4.add(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY2,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION2, 501, 800, EXTENSION))
        );

        committedFiles.put(TOPIC_PARTITION2, list4);

        for (TopicPartition tp : tempfiles.keySet()) {
            for (String file : tempfiles.get(tp)) {
                fs.createNewFile(new Path(file));
            }
        }

        createWALs(tempfiles, committedFiles);
        HdfsSinkTask task = new HdfsSinkTask();

        task.initialize(context);
        task.start(properties);

        Map<TopicPartition, Long> offsets = context.offsets();
        assertEquals(2, offsets.size());
        assertTrue(offsets.containsKey(TOPIC_PARTITION));
        assertEquals(301, (long) offsets.get(TOPIC_PARTITION));
        assertTrue(offsets.containsKey(TOPIC_PARTITION2));
        assertEquals(801, (long) offsets.get(TOPIC_PARTITION2));

        task.stop();
    }

    @Test
    public void testSinkTaskPut() throws Exception {
        setUp();
        HdfsSinkTask task = new HdfsSinkTask();

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        SinkRecord mockRecord =
                new SinkRecord(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), Schema.STRING_SCHEMA, key, schema, record,
                        0);
        SinkRecord processRecord = processorProvider.getProcessor(StoreConstants.TABLES).process(mockRecord);
        Schema processSchema = processRecord.valueSchema();
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
                sinkRecords.add(sinkRecord);
            }
        }
        task.initialize(context);
        task.start(properties);
        task.put(sinkRecords);
        task.stop();

//        AvroData avroData = task.getAvroData();
        avroData = new AvroData(connectorConfig.avroDataConfig());
        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close

        long[] validOffsets = {-1, 2, 5};

        for (TopicPartition tp : context.assignment()) {
            String directory = "partition=" + String.valueOf(tp.partition());
            for (int j = 1; j < validOffsets.length; ++j) {
                long startOffset = validOffsets[j - 1] + 1;
                long endOffset = validOffsets[j];
                Path path = new Path(
                        FileUtils.jointPath(
                                defaultStoreContext.getStoreConfig().tablePath(),
                                directory,
                                defaultStoreContext.getFileOperator().generateCommittedFileName(tp, startOffset, endOffset, EXTENSION))
                );
                Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
                long size = endOffset - startOffset + 1;
                assertEquals(records.size(), size);
                for (Object avroRecord : records) {
                    assertEquals(avroRecord, avroData.fromConnectData(processSchema, processRecord.value()));
                }
            }
        }
    }

    /**
     * not support Primitive, must be Struct.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSinkTaskPutPrimitive() throws Exception {
        setUp();
        HdfsSinkTask task = new HdfsSinkTask();

        final String key = "key";
        final Schema schema = Schema.INT32_SCHEMA;
        final int record = 12;
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
                sinkRecords.add(sinkRecord);
            }
        }
        task.initialize(context);
        task.start(properties);
        task.put(sinkRecords);
        task.stop();

        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = {-1, 2, 5};

        for (TopicPartition tp : context.assignment()) {
            String directory = "partition=" + String.valueOf(tp.partition());
            for (int j = 1; j < validOffsets.length; ++j) {
                long startOffset = validOffsets[j - 1] + 1;
                long endOffset = validOffsets[j];
                Path path = new Path(
                        FileUtils.jointPath(
                                defaultStoreContext.getStoreConfig().tablePath(),
                                directory,
                                defaultStoreContext.getFileOperator().generateCommittedFileName(tp, startOffset, endOffset, EXTENSION))
                );
                Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
                long size = endOffset - startOffset + 1;
                assertEquals(records.size(), size);
                for (Object avroRecord : records) {
                    assertEquals(avroRecord, record);
                }
            }
        }
    }

    private void createCommittedFiles() throws IOException {
        String file1 = FileUtils.jointPath(
                defaultStoreContext.getStoreConfig().tablePath(),
                DIRECTORY1,
                defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 0, 10, EXTENSION)
        );

        String file2 =
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 11, 20, EXTENSION)
                );
        String file3 =
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION2, 21, 40, EXTENSION)
                );
        String file4 =
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        DIRECTORY1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION2, 41, 45, EXTENSION)
                );

        fs.createNewFile(new Path(file1));
        fs.createNewFile(new Path(file2));
        fs.createNewFile(new Path(file3));
        fs.createNewFile(new Path(file4));
    }

    private void createWALs(final Map<TopicPartition, List<String>> tempfiles,
            final Map<TopicPartition, List<String>> committedFiles) throws Exception {
        @SuppressWarnings("unchecked")
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        for (TopicPartition tp : tempfiles.keySet()) {
            WAL wal = storage.wal(defaultStoreContext.getStoreConfig(), tp);
            List<String> tempList = tempfiles.get(tp);
            List<String> committedList = committedFiles.get(tp);
            wal.append(WAL.beginMarker, "");
            for (int i = 0; i < tempList.size(); ++i) {
                wal.append(tempList.get(i), committedList.get(i));
            }
            wal.append(WAL.endMarker, "");
            wal.close();
        }
    }
}
