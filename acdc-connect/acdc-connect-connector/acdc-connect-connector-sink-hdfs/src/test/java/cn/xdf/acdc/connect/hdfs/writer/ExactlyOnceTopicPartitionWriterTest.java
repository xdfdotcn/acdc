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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import cn.xdf.acdc.connect.hdfs.filter.CommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.format.RecordWriterProvider;
import cn.xdf.acdc.connect.hdfs.format.avro.AvroDataFileReader;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.DailyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.DefaultPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.FieldPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.HourlyPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.TimeUtils;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import cn.xdf.acdc.connect.hdfs.wal.FSWAL;
import cn.xdf.acdc.connect.hdfs.wal.WAL;
import cn.xdf.acdc.connect.hdfs.wal.WALFile.Writer;
import cn.xdf.acdc.connect.hdfs.wal.WALFileTest.CorruptWriter;
import io.confluent.common.utils.MockTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ExactlyOnceTopicPartitionWriterTest extends TestWithMiniDFSCluster {

    private RecordWriterProvider writerProvider;

    private RecordWriterProvider
            newWriterProvider;

    private HdfsStorage storage;

    private Map<String, String> localProps = new HashMap<>();

    private MockTime time;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.putAll(localProps);
        return props;
    }

    /**
     * Set up.
     *
     * @throws Exception set up fail
     */
    public void setUp() throws Exception {
        super.setUp();
        time = new MockTime();
        storage = new HdfsStorage(connectorConfig, url);
        writerProvider = null;
        newWriterProvider = defaultStoreContext.getRecordWriterProvider();
        dataFileReader = new AvroDataFileReader();
        extension = newWriterProvider.getExtension();
        createTableDir(defaultStoreContext.getStoreConfig().tablePath());
        createLogsDir(defaultStoreContext.getStoreConfig().walLogPath());
    }

    @Test
    public void testVariablyIncreasingOffsets() throws Exception {
        setUp();
        Partitioner partitioner = new FieldPartitioner();
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);

        @SuppressWarnings("unchecked")
        List<String> partitionFields = (List<String>) parsedConfig.get(
                PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
        );
        String partitionField = partitionFields.get(0);

        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        String key = "key";
        List<SinkRecord> sinkRecords = new ArrayList<>();

        Schema schema = createSchema();
        List<Struct> records = new ArrayList<>();
        int offset = 0;
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                Struct record = createRecord(schema, j, 12.2f);
                records.add(record);
                sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
                offset += 10;
            }
        }
        // Add a single records at the end of the batches sequence
        Struct struct = createRecord(schema);
        sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, struct, offset));
        records.add(struct);
        assertEquals(10, records.size());

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        assertEquals(-1, topicPartitionWriter.offset());
        topicPartitionWriter.recover();
        assertEquals(-1, topicPartitionWriter.offset());

        topicPartitionWriter.write();
        // Flush size is 3, so records with offset 0-80 inclusive are written, and 81 is the next one
        // after the last committed
        assertEquals(81, topicPartitionWriter.offset());
        topicPartitionWriter.close();
        assertEquals(81, topicPartitionWriter.offset());

        Set<Path> expectedFiles = new HashSet<>();
        for (int i = 0; i < records.size() - 1; i++) {
            String encodingPartition = partitionField + "=" + records.get(i).get("int");
            String committedFileName = defaultStoreContext.getFileOperator()
                    .generateCommittedFileName(TOPIC_PARTITION, i * 10, i * 10, extension);
            expectedFiles.add(new Path(
                    FileUtils.jointPath(
                            defaultStoreContext.getStoreConfig().tablePath(),
                            encodingPartition,
                            committedFileName))
            );
        }

        records.sort(Comparator.comparingInt(s -> (int) s.get("int")));
        int expectedBatchSize = 1;
        verify(expectedFiles, expectedBatchSize, records, schema);

        // Try recovering at this point, and check that we've not lost our committed offsets
        topicPartitionWriter.recover();
        assertEquals(81, topicPartitionWriter.offset());
    }

    @Test
    public void testWriteRecordDefaultWithPadding() throws Exception {
        localProps.put(HdfsSinkConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
        setUp();
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

        Set<Path> expectedFiles = new HashSet<>();
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        expectedFiles.add(new Path(storeConfig.tablePath() + "/partition=" + PARTITION
                + "/" + TOPIC_PARTITION.topic() + "+" + PARTITION + "+00+02" + extension));
        expectedFiles.add(new Path(storeConfig.tablePath() + "/partition=" + PARTITION
                + "/" + TOPIC_PARTITION.topic() + "+" + PARTITION + "+03+05" + extension));
        expectedFiles.add(new Path(storeConfig.tablePath() + "/partition=" + PARTITION
                + "/" + TOPIC_PARTITION.topic() + "+" + PARTITION + "+06+08" + extension));
        int expectedBatchSize = 3;
        verify(expectedFiles, expectedBatchSize, records, schema);
    }

    @Test
    public void testWriteRecordDefaultWithPaddingCorruptRecovery() throws Exception {
        localProps.put(HdfsSinkConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
        setUp();

        Partitioner partitioner = new DefaultPartitioner();
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);
        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        //create a corrupt WAL
        WAL wal = new FSWAL(defaultStoreContext.getStoreConfig(), TOPIC_PARTITION, storage) {
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
//        String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
        for (int i = 0; i < 20; ++i) {
            long startOffset = i * 10;
            long endOffset = (i + 1) * 10 - 1;
            String encodingPartition = "partition=" + PARTITION;
            String tempFileName = defaultStoreContext.getFileOperator().generateTempFileName(extension);
            String tempFile = FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(), encodingPartition, tempFileName);
            fs.createNewFile(new Path(tempFile));
            String committedFile = defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, startOffset,
                    endOffset, extension);
            wal.append(tempFile, FileUtils.jointPath(
                    defaultStoreContext.getStoreConfig().tablePath(),
                    encodingPartition,
                    committedFile));
        }
        wal.append(WAL.endMarker, "");
        wal.close();

        topicPartitionWriter.recover();

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 3);
        // Add a single records at the end of the batches sequence. Total records: 10
        records.add(createRecord(schema));
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.write();
        topicPartitionWriter.close();

        Set<Path> expectedFiles = new HashSet<>();
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        expectedFiles.add(new Path(storeConfig.tablePath() + "/partition=" + PARTITION
                + "/" + TOPIC_PARTITION.topic() + "+" + PARTITION + "+00+02" + extension));
        expectedFiles.add(new Path(defaultStoreContext.getStoreConfig().tablePath() + "/partition=" + PARTITION
                + "/" + TOPIC_PARTITION.topic() + "+" + PARTITION + "+03+05" + extension));
        expectedFiles.add(new Path(defaultStoreContext.getStoreConfig().tablePath() + "/partition=" + PARTITION + "/"
                + TOPIC_PARTITION.topic() + "+" + PARTITION + "+06+08" + extension));
        int expectedBatchSize = 3;
        verify(expectedFiles, expectedBatchSize, records, schema);
    }

    @Test
    public void testCloseMultipleTempFiles() throws Exception {
        setUp();
        Partitioner partitioner = new FieldPartitioner();
        partitioner.configure(parsedConfig);

        properties.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "10");
        connectorConfig = new HdfsSinkConfig(properties);
        defaultStoreContext.setPartitioner(partitioner);
        defaultStoreContext.setHdfsSinkConfig(connectorConfig);
        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        Schema schema = createSchema();
        List<Struct> records = new ArrayList<>();
        for (int i = 16; i < 19; ++i) {
            for (int j = 0; j < 2; ++j) {
                records.add(createRecord(schema, i, 12.2f));
            }
        }

        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        assertEquals(-1, topicPartitionWriter.offset());
        topicPartitionWriter.write();
        // Flush size is 10, so records not committed yet
        assertEquals(0, topicPartitionWriter.offset());

        // should not throw
        topicPartitionWriter.close();
    }

    @Test
    public void testWriteRecordFieldPartitioner() throws Exception {
        setUp();
        Partitioner partitioner = new FieldPartitioner();
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);
        @SuppressWarnings("unchecked")
        List<String> partitionFields = (List<String>) parsedConfig.get(
                PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
        );
        String partitionField = partitionFields.get(0);

        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        Schema schema = createSchema();
        List<Struct> records = new ArrayList<>();
        for (int i = 16; i < 19; ++i) {
            for (int j = 0; j < 3; ++j) {
                records.add(createRecord(schema, i, 12.2f));

            }
        }
        // Add a single records at the end of the batches sequence
        records.add(createRecord(schema));
        assertEquals(10, records.size());
        List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        assertEquals(-1, topicPartitionWriter.offset());

        topicPartitionWriter.recover();
        assertEquals(-1, topicPartitionWriter.offset());
        topicPartitionWriter.write();
        // Flush size is 3, so records with offset 0-8 inclusive are written, and 9 is the next one
        // after the last committed
        assertEquals(9, topicPartitionWriter.offset());
        topicPartitionWriter.close();
        assertEquals(9, topicPartitionWriter.offset());

        String directory1 = partitionField + "=" + String.valueOf(16);
        String directory2 = partitionField + "=" + String.valueOf(17);
        String directory3 = partitionField + "=" + String.valueOf(18);

        Set<Path> expectedFiles = new HashSet<>();
        expectedFiles.add(new Path(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        directory1,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 0, 2, extension)))
        );
        expectedFiles.add(new Path(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        directory2,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 3, 5, extension)))
        );
        expectedFiles.add(new Path(
                FileUtils.jointPath(
                        defaultStoreContext.getStoreConfig().tablePath(),
                        directory3,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 6, 8, extension)))
        );
        int expectedBatchSize = 3;
        verify(expectedFiles, expectedBatchSize, records, schema);
        // Try recovering at this point, and check that we've not lost our committed offsets
        topicPartitionWriter.recover();
        assertEquals(9, topicPartitionWriter.offset());
    }

    @Test
    public void testWriteRecordTimeBasedPartition() throws Exception {
        setUp();
        Partitioner partitioner = new TimeBasedPartitioner();
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

        long partitionDurationMs = (Long) parsedConfig.get(
                PartitionerConfig.PARTITION_DURATION_MS_CONFIG
        );
        String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
        String timeZoneString = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
        long timestamp = System.currentTimeMillis();

        String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);

        String directory = encodedPartition;
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();

        Set<Path> expectedFiles = new HashSet<>();
        expectedFiles.add(new Path(
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        directory,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 0, 2, extension)))

        );
        expectedFiles.add(new Path(
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        directory,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 3, 5, extension)))
        );
        expectedFiles.add(new Path(
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        directory,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 6, 8, extension)))

        );
        int expectedBatchSize = 3;
        verify(expectedFiles, expectedBatchSize, records, schema);
    }

    @Test
    public void testWriteRecordTimeBasedPartitionFieldTimestampHours() throws Exception {
        // Do not roll on size, only based on time.
        localProps.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
        localProps.put(
                HdfsSinkConfig.ROTATE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.MINUTES.toMillis(1))
        );
        setUp();

        // Define the partitioner
        partitioner = new HourlyPartitioner();
        parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "RecordField");
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);

        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        Schema schema = createSchemaWithTimestampField();

        DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
        // One record every 20 sec, puts 3 records every minute/rotate interval
        long advanceMs = 20000;
        long timestampFirst = first.getMillis();
        int size = 18;

        ArrayList<Struct> records = new ArrayList<>(size);
        for (int i = 0; i < size / 2; ++i) {
            records.add(createRecordWithTimestampField(schema, timestampFirst));
            timestampFirst += advanceMs;
        }
        Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), schema);

        long timestampLater = first.plusHours(2).getMillis();
        for (int i = size / 2; i < size; ++i) {
            records.add(createRecordWithTimestampField(schema, timestampLater));
            timestampLater += advanceMs;
        }
        sinkRecords.addAll(createSinkRecords(
                records.subList(9, 18),
                schema,
                9,
                Collections.singleton(new TopicPartition(TOPIC, PARTITION))
        ));

        // And one last record to flush the previous ones.
        long timestampMuchLater = first.plusHours(6).getMillis();
        Struct lastOne = createRecordWithTimestampField(schema, timestampMuchLater);
        sinkRecords.addAll(createSinkRecords(
                Collections.singletonList(lastOne),
                schema,
                19,
                Collections.singleton(new TopicPartition(TOPIC, PARTITION))
        ));

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.recover();
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
        String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

        String dirPrefixFirst = encodedPartitionFirst;
        Set<Path> expectedFiles = new HashSet<>();
        for (int i : new int[]{0, 3, 6}) {
            expectedFiles.add(new Path(
                    FileUtils.jointPath(
                            defaultStoreContext.getStoreConfig().tablePath(),
                            dirPrefixFirst,
                            defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension)
                    )
            ));
        }

        String dirPrefixLater = encodedPartitionLater;
        for (int i : new int[]{9, 12, 15}) {
            expectedFiles.add(new Path(
                    FileUtils.jointPath(
                            defaultStoreContext.getStoreConfig().tablePath(),
                            dirPrefixLater),
                    defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension)
            ));

        }
        verify(expectedFiles, 3, records, schema);
    }

    @Test
    public void testWriteRecordTimeBasedPartitionRecordTimestampHours() throws Exception {
        // Do not roll on size, only based on time.
        localProps.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
        localProps.put(
                HdfsSinkConfig.ROTATE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.MINUTES.toMillis(1))
        );
        setUp();

        // Define the partitioner
        partitioner = new HourlyPartitioner<FieldSchema>();
        parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);

        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 6);
        DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
        // One record every 20 sec, puts 3 records every minute/rotate interval
        long advanceMs = 20000;
        long timestampFirst = first.getMillis();
        Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(records.subList(0, 9), schema, 0, timestampFirst, advanceMs);
        long timestampLater = first.plusHours(2).getMillis();
        sinkRecords.addAll(createSinkRecordsWithTimestamp(records.subList(9, 18), schema, 9, timestampLater, advanceMs));

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.recover();
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
        String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);
        //

        String dirPrefixFirst = encodedPartitionFirst;
        Set<Path> expectedFiles = new HashSet<>();
        for (int i : new int[]{0, 3, 6}) {
            String commitFile = defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension);
            expectedFiles.add(new Path(FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(), dirPrefixFirst, commitFile)));
        }

        String dirPrefixLater = encodedPartitionLater;
        // Records 15,16,17 won't be flushed until a record with a higher timestamp arrives.
        for (int i : new int[]{9, 12}) {
            String commitFile = defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension);
            expectedFiles.add(new Path(FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(), dirPrefixLater, commitFile)));
        }
        verify(expectedFiles, 3, records, schema);
    }

    @Test
    public void testWriteRecordTimeBasedPartitionRecordTimestampDays() throws Exception {
        // Do not roll on size, only based on time.
        localProps.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
        localProps.put(
                HdfsSinkConfig.ROTATE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.MINUTES.toMillis(1))
        );
        setUp();

        // Define the partitioner
        partitioner = new DailyPartitioner();
        parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
        parsedConfig.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY/'month'=MM/'day'=dd");
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);

        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                time,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 6);
        DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
        // One record every 20 sec, puts 3 records every minute/rotate interval
        long advanceMs = 20000;
        long timestampFirst = first.getMillis();
        Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(records.subList(0, 9), schema, 0, timestampFirst, advanceMs);
        long timestampLater = first.plusHours(2).getMillis();
        sinkRecords.addAll(createSinkRecordsWithTimestamp(records.subList(9, 18), schema, 9, timestampLater, advanceMs));

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.recover();
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
        String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

        String dirPrefixFirst = encodedPartitionFirst;
        Set<Path> expectedFiles = new HashSet<>();
        for (int i : new int[]{0, 3, 6}) {
            expectedFiles.add(new Path(
                    FileUtils.jointPath(
                            defaultStoreContext.getStoreConfig().tablePath(),
                            dirPrefixFirst,
                            defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension)
                    )
            ));
        }

        String dirPrefixLater = encodedPartitionLater;
        // Records 15,16,17 won't be flushed until a record with a higher timestamp arrives.
        for (int i : new int[]{9, 12}) {
            expectedFiles.add(new Path(
                    FileUtils.jointPath(
                            defaultStoreContext.getStoreConfig().tablePath(),
                            dirPrefixLater),
                    defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension)
            ));
        }
        verify(expectedFiles, 3, records, schema);
    }

    @Test
    public void testWriteRecordTimeBasedPartitionWallclockMockedWithScheduleRotation()
            throws Exception {
        // Do not roll on size, only based on time.
        localProps.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
        localProps.put(
                HdfsSinkConfig.ROTATE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.HOURS.toMillis(1))
        );
        localProps.put(
                HdfsSinkConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.MINUTES.toMillis(10))
        );
        setUp();

        // Define the partitioner
        partitioner = new TimeBasedPartitioner();
        parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
        parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockedWallclockTimestampExtractor.class.getName());
        partitioner.configure(parsedConfig);
        defaultStoreContext.setPartitioner(partitioner);
        MockedWallclockTimestampExtractor.TIME.sleep(Time.SYSTEM.milliseconds());

        TopicPartitionWriter topicPartitionWriter = new ExactlyOnceTopicPartitionWriter(
                context,
                MockedWallclockTimestampExtractor.TIME,
                TOPIC_PARTITION,
                defaultStoreContext
        );

        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 6);
        Collection<SinkRecord> sinkRecords = createSinkRecords(
                records.subList(0, 3),
                schema,
                0,
                Collections.singleton(new TopicPartition(TOPIC, PARTITION))
        );
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.recover();
        topicPartitionWriter.write();
        long timestampFirst = MockedWallclockTimestampExtractor.TIME.milliseconds();

        // 11 minutes
        MockedWallclockTimestampExtractor.TIME.sleep(TimeUnit.MINUTES.toMillis(11));
        // Records are written due to scheduled rotation
        topicPartitionWriter.write();

        sinkRecords = createSinkRecords(
                records.subList(3, 6),
                schema,
                3,
                Collections.singleton(new TopicPartition(TOPIC, PARTITION))
        );
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        // More records later
        topicPartitionWriter.write();
        long timestampLater = MockedWallclockTimestampExtractor.TIME.milliseconds();

        // 11 minutes later, another scheduled rotation
        MockedWallclockTimestampExtractor.TIME.sleep(TimeUnit.MINUTES.toMillis(11));

        // Again the records are written due to scheduled rotation
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
        String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

        String dirPrefixFirst = encodedPartitionFirst;
        Set<Path> expectedFiles = new HashSet<>();
        for (int i : new int[]{0}) {
            expectedFiles.add(new Path(
                    FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(), dirPrefixFirst,
                            defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension)))
            );
        }

        String dirPrefixLater = encodedPartitionLater;
        for (int i : new int[]{3}) {
            expectedFiles.add(new Path(
                    FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(), dirPrefixLater,
                            defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, i, i + 2, extension))
            ));
        }
        verify(expectedFiles, 3, records, schema);
    }

    private String getTimebasedEncodedPartition(long timestamp) {
        long partitionDurationMs = (Long) parsedConfig.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
        String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
        String timeZone = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
        return TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZone, timestamp);
    }

    private void createTableDir(final String tablePath) throws IOException {
        Path path = new Path(tablePath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }

    private void createLogsDir(final String walLogPath) throws IOException {
        Path path = new Path(walLogPath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }

    private void verify(final Set<Path> expectedFiles, int expectedSize, final List<Struct> records, final Schema schema) throws IOException {
        Path path = new Path(defaultStoreContext.getStoreConfig().tablePath());
        FileStatus[] statuses = FileUtils.traverse(storage, path, new CommittedFileFilter());
        assertEquals(expectedFiles.size(), statuses.length);
        int index = 0;
        for (FileStatus status : statuses) {
            Path filePath = status.getPath();
            assertTrue(expectedFiles.contains(status.getPath()));
            Collection<Object> avroRecords = dataFileReader.readData(connectorConfig.getHadoopConfiguration(), filePath);
            assertEquals(expectedSize, avroRecords.size());
            for (Object avroRecord : avroRecords) {
                assertEquals(avroData.fromConnectData(schema, records.get(index++)), avroRecord);
            }
        }
    }

    public static class MockedWallclockTimestampExtractor
            extends
            TimeBasedPartitioner.WallclockTimestampExtractor {

        public static final MockTime TIME = new MockTime();

        @Override
        public void configure(final Map<String, Object> config) {
        }

        @Override
        public Long extract(final ConnectRecord<?> record) {
            return TIME.milliseconds();
        }
    }
}
