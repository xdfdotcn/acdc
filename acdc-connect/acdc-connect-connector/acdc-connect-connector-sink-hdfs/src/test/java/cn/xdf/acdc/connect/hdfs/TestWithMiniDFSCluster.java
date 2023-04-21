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

import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.filter.TableTopicPartitionCommittedFileFilter;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import static org.hamcrest.CoreMatchers.is;

import org.junit.After;
import org.junit.AfterClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.BeforeClass;

public class TestWithMiniDFSCluster extends HdfsSinkConnectorTestBase {

    // CHECKSTYLE:OFF

    protected static FileSystem fs;

    private static MiniDFSCluster cluster;

    protected DataFileReader dataFileReader;

    protected Partitioner partitioner;

    protected String extension;

    private Map<String, String> localProps = new HashMap<>();


    @BeforeClass
    public static void setup() throws IOException {
        cluster = createDFSCluster();
        fs = cluster.getFileSystem();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown(true);
        }
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
        localProps.put(HdfsSinkConfig.HDFS_URL_CONFIG, url);
        localProps.put(StorageCommonConfig.STORE_URL_CONFIG, url);
        props.putAll(localProps);
        return props;
    }

    /**
     * should be omitted in order to be able to add properties per test.
     *
     * @throws Exception exception on set up
     */
    public void setUp() throws Exception {
        super.setUp();
    }

    private StoreContext createStoreContext() throws IOException {
        return StoreContext.buildContext(connectorConfig);
    }

    @After
    public void tearDown() throws Exception {
        clearDFS();
    }

    /**
     * Return a list of new records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createSinkRecords(int size) {
        return createSinkRecords(size, 0);
    }

    /**
     * Return a list of new records starting at the given offset.
     *
     * @param size        the number of records to return.
     * @param startOffset the starting offset.
     * @return the list of records.
     */
    protected List<SinkRecord> createSinkRecords(int size, long startOffset) {
        return createSinkRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    }

    /**
     * Return a list of new records for a set of partitions, starting at the given offset in each partition.
     *
     * @param size        the number of records to return.
     * @param startOffset the starting offset.
     * @param partitions  the set of partitions to create records for.
     * @return the list of records.
     */
    protected List<SinkRecord> createSinkRecords(int size, long startOffset, final Set<TopicPartition> partitions) {
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        List<Struct> same = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            same.add(record);
        }
        return createSinkRecords(same, schema, startOffset, partitions);
    }

    protected List<SinkRecord> createSinkRecords(final List<Struct> records, final Schema schema) {
        return createSinkRecords(records, schema, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    }

    protected List<SinkRecord> createSinkRecords(final List<Struct> records, final Schema schema, long startOffset,
            final Set<TopicPartition> partitions) {
        String key = "key";
        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            long offset = startOffset;
            for (Struct record : records) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset++));
            }
        }
        return sinkRecords;
    }

    protected List<SinkRecord> createSinkRecordsNoVersion(int size, long startOffset) {
        String key = "key";
        Schema schemaNoVersion = SchemaBuilder.struct().name("record")
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .build();

        Struct recordNoVersion = new Struct(schemaNoVersion);
        recordNoVersion.put("boolean", true)
                .put("int", 12)
                .put("long", 12L)
                .put("float", 12.2f)
                .put("double", 12.2);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset; offset < startOffset + size; ++offset) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion,
                    recordNoVersion, offset));
        }
        return sinkRecords;
    }

    protected List<SinkRecord> createSinkRecordsWithAlternatingSchemas(int size, long startOffset) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        Schema newSchema = createNewSchema();
        Struct newRecord = createNewRecord(newSchema);

        int limit = (size / 2) * 2;
        boolean remainder = size % 2 > 0;
        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset; offset < startOffset + limit; ++offset) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
            // CHECKSTYLE:OFF
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, ++offset));
        }
        if (remainder) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                    startOffset + size - 1));
        }
        return sinkRecords;
    }

    protected List<SinkRecord> createSinkRecordsInterleaved(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset, total = 0; total < size; ++offset) {
            for (TopicPartition tp : partitions) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
                if (++total >= size) {
                    break;
                }
            }
        }
        return sinkRecords;
    }

    // Given a list of records, create a list of sink records with contiguous offsets.
    protected List<SinkRecord> createSinkRecordsWithTimestamp(
            final List<Struct> records,
            final Schema schema,
            int startOffset,
            long startTime,
            long timeStep
    ) {
        String key = "key";
        ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 0, offset = startOffset; i < records.size(); ++i, ++offset) {
            sinkRecords.add(new SinkRecord(
                    TOPIC,
                    PARTITION,
                    Schema.STRING_SCHEMA,
                    key,
                    schema,
                    records.get(i),
                    offset,
                    startTime + offset * timeStep,
                    TimestampType.CREATE_TIME
            ));
        }
        return sinkRecords;
    }

    @Deprecated
    protected String getDirectory() {
        return getDirectory(PARTITION);
    }

    @Deprecated
    protected String getDirectory(int partition) {
        String encodedPartition = "partition=" + String.valueOf(partition);
        return encodedPartition;
//        return partitioner.generatePartitionedPath(tableName, encodedPartition);
    }


    protected String getEncodingPartition(int partition) {
        String encodedPartition = "partition=" + String.valueOf(partition);
        return encodedPartition;
    }


    protected void verify(final List<SinkRecord> sinkRecords, final long[] validOffsets) throws IOException {
        verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
    }

    protected void verify(final List<SinkRecord> sinkRecords, final long[] validOffsets, final Set<TopicPartition> partitions)
            throws IOException {
        verify(sinkRecords, validOffsets, partitions, false);
    }

    /**
     * Verify files and records are uploaded appropriately.
     *
     * @param sinkRecords  a flat list of the records that need to appear in potentially several * files in HDFS.
     * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition. Offsets appear in ascending order, the difference between two consecutive offsets equals
     *                     the expected size of the file, and last offset is exclusive.
     * @param partitions   the set of partitions to verify records for.
     */
    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
            boolean skipFileListing) throws IOException {
        if (!skipFileListing) {
            verifyFileListing(validOffsets, partitions);
        }

        for (TopicPartition tp : partitions) {
            for (int i = 1, j = 0; i < validOffsets.length; ++i) {
                long startOffset = validOffsets[i - 1];
                long endOffset = validOffsets[i] - 1;

//        String topicsDir = this.topicsDir.get(tp.topic());
                String partitionPath = FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(),
                        getEncodingPartition(tp.partition()));

                String filename = defaultStoreContext.getFileOperator().generateCommittedFileName(tp,
                        startOffset, endOffset, extension);
                Path path = new Path(FileUtils.jointPath(partitionPath, filename));
                Collection<Object> records = dataFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
                long size = endOffset - startOffset + 1;
                assertEquals(size, records.size());
                verifyContents(sinkRecords, j, records);
                j += size;
            }
        }
    }

    protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp) {
        List<String> expectedFiles = new ArrayList<>();
        for (int i = 1; i < validOffsets.length; ++i) {
            long startOffset = validOffsets[i - 1];
            long endOffset = validOffsets[i] - 1;
//            String topicsDir = this.topicsDir.get(tp.topic());
            String partitionPath = FileUtils.jointPath(defaultStoreContext.getStoreConfig().tablePath(),
                    getEncodingPartition(tp.partition()));

            String commitFileName = defaultStoreContext.getFileOperator().generateCommittedFileName(tp,
                    startOffset, endOffset, extension);

            expectedFiles.add(FileUtils.jointPath(partitionPath, commitFileName));
        }
        return expectedFiles;
    }

    protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
        for (TopicPartition tp : partitions) {
            verifyFileListing(getExpectedFiles(validOffsets, tp), tp);
        }
    }

    protected void verifyFileListing(List<String> expectedFiles, TopicPartition tp) throws IOException {
        FileStatus[] statuses = {};
        try {
//            String topicsDir = this.topicsDir.get(tp.topic());
            String partitionPath = FilePath.of(defaultStoreContext.getStoreConfig().tablePath())
                    .join("partition=" + tp.partition()).build().path();
            statuses = fs.listStatus(
                    new Path(partitionPath),
                    new TableTopicPartitionCommittedFileFilter(tp));
        } catch (FileNotFoundException e) {
            // the directory does not exist.
        }

        List<String> actualFiles = new ArrayList<>();
        for (FileStatus status : statuses) {
            actualFiles.add(status.getPath().toString());
        }

        Collections.sort(actualFiles);
        Collections.sort(expectedFiles);
        assertThat(actualFiles, is(expectedFiles));
    }

    protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
        Schema expectedSchema = null;
        for (Object avroRecord : records) {
            if (expectedSchema == null) {
                expectedSchema = expectedRecords.get(startIndex).valueSchema();
            }
            Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                    expectedRecords.get(startIndex++).value(),
                    expectedSchema);
            assertEquals(avroData.fromConnectData(expectedSchema, expectedValue), avroRecord);
        }
    }

    private static MiniDFSCluster createDFSCluster() throws IOException {
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
                .hosts(new String[]{"localhost", "localhost", "localhost"})
                .nameNodePort(9001)
                .numDataNodes(3)
                .build();
        cluster.waitActive();

        return cluster;
    }

    protected void clearDFS() throws IOException {
        if (fs.exists(new Path(StoreConstants.HDFS_ROOT)) && fs.isDirectory(new Path(StoreConstants.HDFS_ROOT))) {
            for (FileStatus file : fs.listStatus(new Path(StoreConstants.HDFS_ROOT))) {
                if (file.isDirectory()) {
                    fs.delete(file.getPath(), true);
                } else {
                    fs.delete(file.getPath(), false);
                }
            }
        }
    }
}
