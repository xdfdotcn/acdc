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

package cn.xdf.acdc.connect.hdfs.format.avro;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.wal.FSWAL;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import cn.xdf.acdc.connect.hdfs.wal.WALFile.Writer;
import cn.xdf.acdc.connect.hdfs.wal.WALFileTest.CorruptWriter;
import cn.xdf.acdc.connect.hdfs.writer.ExactlyOnceTopicPartitionWriterTest;
import io.confluent.common.utils.Time;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

@Slf4j
public class DataWriterAvroTest extends TestWithMiniDFSCluster {

    private static final String ROTATE_INTERVAL_MS_CONFIG = "1000";

    // wait for 2 * ROTATE_INTERVAL_MS_CONFIG
    private static final long WAIT_TIME = Long.valueOf(ROTATE_INTERVAL_MS_CONFIG) * 2;

    private static final String FLUSH_SIZE_CONFIG = "10";

    // send 1.5 * FLUSH_SIZE_CONFIG records
    private static final int NUMBER_OF_RECORDS = Integer.valueOf(FLUSH_SIZE_CONFIG) + Integer.valueOf(FLUSH_SIZE_CONFIG) / 2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataFileReader = new AvroDataFileReader();
        extension = ".avro";
    }

    @Test
    public void testWriteRecord() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(7);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testRecovery() throws Exception {
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        HdfsFileOperator fileOperator = defaultStoreContext.getFileOperator();
        fs.delete(new Path(FilePath.of(storeConfig.tablePath())
                        .join(String.valueOf(TOPIC_PARTITION.partition()))
                        .build().path()
                ),
                true
        );

        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        partitioner = hdfsWriter.getPartitioner();

        WAL wal = storage.wal(storeConfig, TOPIC_PARTITION);

        wal.append(WAL.beginMarker, "");

        for (int i = 0; i < 5; ++i) {
            long startOffset = i * 10;
            long endOffset = (i + 1) * 10 - 1;
            String tempfile = FilePath.of(storeConfig.tablePath())
                    .join(getDirectory())
                    .join(fileOperator.generateTempFileName(extension))
                    .build().path();
            fs.createNewFile(new Path(tempfile));
            String committedFile = FilePath.of(storeConfig.tablePath())
                    .join(getDirectory())
                    .join(fileOperator.generateCommittedFileName(TOPIC_PARTITION, startOffset, endOffset, extension))
                    .build().path();
            wal.append(tempfile, committedFile);
        }
        wal.append(WAL.endMarker, "");
        wal.close();

        hdfsWriter.recover(TOPIC_PARTITION);
        Map<TopicPartition, Long> offsets = context.offsets();
        assertTrue(offsets.containsKey(TOPIC_PARTITION));
        assertEquals(50L, (long) offsets.get(TOPIC_PARTITION));

        List<SinkRecord> sinkRecords = createSinkRecords(3, 50);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        long[] validOffsets = {0, 10, 20, 30, 40, 50, 53};
        verifyFileListing(validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    }

    @Test
    public void testCorruptRecovery() throws Exception {
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        HdfsFileOperator fileOperator = defaultStoreContext.getFileOperator();
        fs.delete(new Path(FilePath.of(storeConfig.tablePath())
                        .join(String.valueOf(TOPIC_PARTITION.partition()))
                        .build().path()
                ),
                true
        );

        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();

        WAL wal = new FSWAL(storeConfig, TOPIC_PARTITION, storage) {
            public void acquireLease() throws ConnectException {
                super.acquireLease();
                // initialize a new writer if the writer is not a CorruptWriter
                if (getWriter().getClass() != CorruptWriter.class) {
                    try {
                        setWriter(new CorruptWriter(storage.conf(), Writer.file(new Path(this.getLogFile())),
                                Writer.appendIfExists(true)));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        wal.append(WAL.beginMarker, "");

        // Write enough bytes to trigger a sync
        for (int i = 0; i < 20; ++i) {
            long startOffset = i * 10;
            long endOffset = (i + 1) * 10 - 1;
            String tempfile = FilePath.of(storeConfig.tablePath())
                    .join(getDirectory())
                    .join(fileOperator.generateTempFileName(extension))
                    .build().path();
            fs.createNewFile(new Path(tempfile));
            String committedFile = FilePath.of(storeConfig.tablePath())
                    .join(getDirectory())
                    .join(fileOperator.generateCommittedFileName(TOPIC_PARTITION, startOffset, endOffset, extension))
                    .build().path();

            wal.append(tempfile, committedFile);
        }

        wal.append(WAL.endMarker, "");
        wal.close();

        hdfsWriter.recover(TOPIC_PARTITION);
        Map<TopicPartition, Long> offsets = context.offsets();
        // Offsets shouldn't exist since corrupt WAL file entries should not not be committed
        assertFalse(offsets.containsKey(TOPIC_PARTITION));
    }

    @Test
    public void testWriteRecordMultiplePartitions() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();

        for (TopicPartition tp : context.assignment()) {
            hdfsWriter.recover(tp);
        }

        List<SinkRecord> sinkRecords = createSinkRecords(7, 0, context.assignment());

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, context.assignment());
    }

    @Test
    public void testWriteInterleavedRecordsInMultiplePartitions() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();

        for (TopicPartition tp : context.assignment()) {
            hdfsWriter.recover(tp);
        }

        List<SinkRecord> sinkRecords = createSinkRecordsInterleaved(7 * context.assignment().size(), 0,
                context.assignment());

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, context.assignment());
    }

    @Test
    public void testWriteInterleavedRecordsInMultiplePartitionsNonZeroInitialOffset() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();

        List<SinkRecord> sinkRecords = createSinkRecordsInterleaved(7 * context.assignment().size(), 9,
                context.assignment());

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        long[] validOffsets = {9, 12, 15};
        verify(sinkRecords, validOffsets, context.assignment());
    }

    @Test
    public void testGetNextOffsets() throws Exception {
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        HdfsFileOperator fileOperator = defaultStoreContext.getFileOperator();
        String directory = "partition=" + String.valueOf(PARTITION);
        long[] startOffsets = {0, 3};
        long[] endOffsets = {2, 5};

        for (int i = 0; i < startOffsets.length; ++i) {
            Path path = new Path(
                    fileOperator.createCommittedFileInTablePartitionPath(
                            directory,
                            TOPIC_PARTITION,
                            startOffsets[i],
                            endOffsets[i],
                            extension
                    ));
            fs.createNewFile(path);
        }
        Path path = new Path(fileOperator.createTempFileInTempTablePartitionPath(directory, extension));
        fs.createNewFile(path);

        path = new Path(FilePath.of(storeConfig.tablePath())
                .join(directory)
                .join("abcd")
                .build().path()
        );
        fs.createNewFile(path);

        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);

        hdfsWriter.recover(TOPIC_PARTITION);

        Map<TopicPartition, Long> committedOffsets = hdfsWriter.getCommittedOffsets();

        assertTrue(committedOffsets.containsKey(TOPIC_PARTITION));
        long nextOffset = committedOffsets.get(TOPIC_PARTITION);
        assertEquals(6L, nextOffset);

        hdfsWriter.close();
        hdfsWriter.stop();
    }

    @Test
    public void testWriteRecordNonZeroInitialOffset() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(7, 3);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = {3, 6, 9};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testRebalance() throws Exception {
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();

        Set<TopicPartition> originalAssignment = new HashSet<>(context.assignment());
        // Starts with TOPIC_PARTITION and TOPIC_PARTITION2
        for (TopicPartition tp : originalAssignment) {
            hdfsWriter.recover(tp);
        }

        Set<TopicPartition> nextAssignment = new HashSet<>();
        nextAssignment.add(TOPIC_PARTITION);
        nextAssignment.add(TOPIC_PARTITION3);

        List<SinkRecord> sinkRecords = createSinkRecords(7, 0, originalAssignment);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        // Set the new assignment to the context
        context.setAssignment(nextAssignment);
        hdfsWriter.open(nextAssignment);

        assertEquals(null, hdfsWriter.getBucketWriter(TOPIC_PARTITION2));
        assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION));
        assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION3));

        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsetsTopicPartition2 = {0, 3, 6};
        verify(sinkRecords, validOffsetsTopicPartition2, Collections.singleton(TOPIC_PARTITION2), true);

        // Message offsets start at 6 because we discarded the in-progress temp file on re-balance
        sinkRecords = createSinkRecords(3, 6, context.assignment());

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsetsTopicPartition1 = {6, 9};
        verify(sinkRecords, validOffsetsTopicPartition1, Collections.singleton(TOPIC_PARTITION), true);

        long[] validOffsetsTopicPartition3 = {6, 9};
        verify(sinkRecords, validOffsetsTopicPartition3, Collections.singleton(TOPIC_PARTITION3), true);
    }

    @Test
    public void testProjectBackWard() throws Exception {
        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "2");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecordsWithAlternatingSchemas(7, 0);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();
        long[] validOffsets = {0, 1, 3, 5, 7};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testProjectNone() throws Exception {
        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "2");
        HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecordsWithAlternatingSchemas(7, 0);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        long[] validOffsets = {0, 1, 2, 3, 4, 5, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testProjectForward() throws Exception {
        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, "2");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
        HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        // By excluding the first element we get a list starting with record having the new schema.
        List<SinkRecord> sinkRecords = createSinkRecordsWithAlternatingSchemas(8, 0).subList(1, 8);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        long[] validOffsets = {1, 2, 4, 6, 8};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testProjectNoVersion() throws Exception {
        Map<String, String> props = createProps();
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecordsNoVersion(1, 0);
        sinkRecords.addAll(createSinkRecordsWithAlternatingSchemas(7, 0));

        try {
            hdfsWriter.write(sinkRecords);
            fail("Version is required for Backward compatibility.");
            // CHECKSTYLE:OFF
        } catch (RuntimeException e) {
            // expected
        } finally {
            hdfsWriter.close();
            hdfsWriter.stop();
            long[] validOffsets = {};
            verify(Collections.<SinkRecord>emptyList(), validOffsets);
        }
    }

    @Test
    public void testFlushPartialFile() throws Exception {

        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.FLUSH_SIZE_CONFIG, FLUSH_SIZE_CONFIG);
        props.put(HdfsSinkConfig.ROTATE_INTERVAL_MS_CONFIG, ROTATE_INTERVAL_MS_CONFIG);
        props.put(
                PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
                String.valueOf(TimeUnit.DAYS.toMillis(1))
        );
        props.put(
                PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                ExactlyOnceTopicPartitionWriterTest.MockedWallclockTimestampExtractor.class.getName()
        );
        props.put(
                PartitionerConfig.PARTITIONER_CLASS_CONFIG,
                TimeBasedPartitioner.class.getName()
        );
        HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
        context.assignment().add(TOPIC_PARTITION);

        Time time = ExactlyOnceTopicPartitionWriterTest.MockedWallclockTimestampExtractor.TIME;
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context, time);
        partitioner = hdfsWriter.getPartitioner();

        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createSinkRecords(NUMBER_OF_RECORDS);
        hdfsWriter.write(sinkRecords);

        // Wait so everything is committed
        time.sleep(WAIT_TIME);
        hdfsWriter.write(new ArrayList<SinkRecord>());

        Map<TopicPartition, Long> committedOffsets = hdfsWriter.getCommittedOffsets();
        assertTrue(committedOffsets.containsKey(TOPIC_PARTITION));
        long nextOffset = committedOffsets.get(TOPIC_PARTITION);
        assertEquals(NUMBER_OF_RECORDS, nextOffset);

        hdfsWriter.close();
        hdfsWriter.stop();
    }

    @Test
    public void testAvroCompression() throws Exception {
        //set compression codec to Snappy
        Map<String, String> props = createProps();
        props.put(HdfsSinkConfig.AVRO_CODEC_CONFIG, "snappy");
        HdfsSinkConfig connectorConfig = new HdfsSinkConfig(props);
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);
        List<SinkRecord> sinkRecords = createSinkRecords(7);

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);

        // check if the raw bytes have a "avro.codec" entry followed by "snappy"
        List<String> filenames = getExpectedFiles(validOffsets, TOPIC_PARTITION);
        for (String filename : filenames) {
            Path p = new Path(filename);
            try (FSDataInputStream stream = fs.open(p)) {
                int size = (int) fs.getFileStatus(p).getLen();
                ByteBuffer buffer = ByteBuffer.allocate(size);
                if (stream.read(buffer) <= 0) {
                    log.error("Could not read file {}", filename);
                }

                String fileContents = new String(buffer.array());
                int index;
                assertTrue((index = fileContents.indexOf("avro.codec")) > 0
                        && fileContents.indexOf("snappy", index) > 0);
            }
        }
    }
}
