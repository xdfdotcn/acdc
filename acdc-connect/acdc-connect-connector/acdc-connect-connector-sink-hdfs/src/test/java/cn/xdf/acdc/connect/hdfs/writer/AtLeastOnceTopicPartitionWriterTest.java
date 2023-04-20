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
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.initialize.StorageMode;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.rotation.RotationPolicyType;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * {@link AtLeastOnceTopicPartitionWriter}.
 */
public class AtLeastOnceTopicPartitionWriterTest extends HiveTestBase {

    private Map<String, String> localProps = new HashMap<>();

    private StoreContext storeContext;

    private AtLeastOnceTopicPartitionWriter tpWriter;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
                "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        localProps.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        localProps.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        localProps.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        localProps.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        localProps.put(HdfsSinkConfig.ROTATION_POLICY, RotationPolicyType.FILE_SIZE.name());

        // hive integration mode
        localProps.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.AT_LEAST_ONCE.name());
        // store mode
        localProps.put(HdfsSinkConfig.STORAGE_FORMAT, Format.TEXT.name());
        localProps.put(HdfsSinkConfig.STORAGE_ROOT_PATH, StoreConstants.HDFS_ROOT);
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
        super.cleanHive();
        super.createDefaultTable();
        this.storeContext = defaultStoreContext;
        this.tpWriter = new AtLeastOnceTopicPartitionWriter(
                null,
                TOPIC_PARTITION,
                this.storeContext
        );
    }

    @Test
    public void testWriteRecordWithoutCommit() throws Exception {
        setUp();
        List<SinkRecord> recordList = createSinkRecords(1);
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();
        List<FileStatus> fileStatuses = storeContext.getFileOperator()
                .storage()
                .list(storeContext.getStoreConfig().tablePath());
        assertTrue(fileStatuses.size() != 0);
        for (FileStatus fileStatus : fileStatuses) {
            assertTrue(fileStatus.getLen() == 0);
        }
    }

    @Test
    public void testWriteRecordWithCommit() throws Exception {
        setUp();
        List<SinkRecord> recordList = createSinkRecords(1);
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();
        tpWriter.commit();
        Partitioner partitioner = storeContext.getPartitioner();
        List<FileStatus> notCloseFileList = storeContext.getFileOperator()
                .storage()
                .list(FilePath.of(storeContext.getStoreConfig().tablePath())
                        .join(partitioner.encodePartition(null))
                        .build().path()
                );
        for (FileStatus fileStatus : notCloseFileList) {
//            assertTrue(fileStatus.getLen() == 0);
            assertTrue(fileStatus.getLen() != 0);
        }

//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            List<FileStatus> jvmShutDownFileList = storeContext.getFileOperator()
//                    .storage()
//                    .list(storeContext.getStoreConfig().tablePath());
//            for (FileStatus fileStatus : jvmShutDownFileList) {
//            }
//        }));
    }

    @Test(expected = ConnectException.class)
    public void testWriteShouldShouldThrownExceptionWithAppendFileAlreadyBeingCreated() throws Exception {
        setUp();
        List<SinkRecord> recordList = createSinkRecords(1);
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();
//        tpWriter.commit();
        // this case is not be happen if  not invoke close method
        tpWriter = new AtLeastOnceTopicPartitionWriter(
                null,
                TOPIC_PARTITION,
                storeContext
        );

        recordList = createSinkRecords(2);
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();
    }

    @Test
    public void testWriteShouldFlushDataToHDFSWhenCommit() throws Exception {
        setUp();
        List<SinkRecord> recordList = createSinkRecords(1);
        Partitioner partitioner = storeContext.getPartitioner();
        HdfsFileOperator fileOperator = storeContext.getFileOperator();
        StoreConfig storeConfig = storeContext.getStoreConfig();
        String expectCommitFile = FilePath.of(storeConfig.tablePath())
                .join(partitioner.encodePartition(null))
                .join(fileOperator.generateCommittedFileName(
                        TOPIC_PARTITION,
                        0,
                        1,
                        ".txt"
                )).build().path();
        // write 1
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();
        String partition = partitioner.encodePartition(null);
        assertTrue(tpWriter.getEncodePartitionWriters().size() == 1);
        assertTrue(null != tpWriter.getEncodePartitionWriters().get(partition));
        long firstWriteFileSize = tpWriter.getEncodePartitionWriters().get(partition).fileSize();
        String firstWriteFileName = tpWriter.getEncodePartitionWriters().get(partition).fileName();
        assertEquals(expectCommitFile, firstWriteFileName);
        tpWriter.commit();
        tpWriter.close();
        assertTrue(tpWriter.getEncodePartitionWriters().size() == 0);
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();
        long secondWriteFileSize = tpWriter.getEncodePartitionWriters().get(partition).fileSize();
        String secondWriteFileName = tpWriter.getEncodePartitionWriters().get(partition).fileName();
        assertEquals(expectCommitFile, secondWriteFileName);
        assertTrue(secondWriteFileSize > firstWriteFileSize);
    }

    /*
     no commit
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000001.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000002.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000003.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000004.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000005.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000006.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000007.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000008.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000009.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000010.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000011.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000012.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000013.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000014.txt
     /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000015.txt
     +------+--+
    | _c0  |
    +------+--+
    | 14   |
    +------+--+
    commit
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000001.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000002.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000003.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000004.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000005.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000006.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000007.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000008.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000009.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000010.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000011.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000012.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000013.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000014.txt
    /unit_test/default/test_sink/dt=20210729/test_sink+12+0000000000+0000000015.txt
    +------+--+
    | _c0  |
    +------+--+
    | 15   |
    +------+--+
     */
    @Test
    public void testWriteShouldRotate() throws Exception {
        // rotation file size 10B
        localProps.put(HdfsSinkConfig.ROTATION_POLICY_FILE_SIZE, "10");
        setUp();
        List<SinkRecord> recordList = createSinkRecords(5);
        Partitioner partitioner = storeContext.getPartitioner();
        HdfsFileOperator fileOperator = storeContext.getFileOperator();
        StoreConfig storeConfig = storeContext.getStoreConfig();
        String partition = partitioner.encodePartition(null);
        List<String> expectFileList = new ArrayList<>();
        expectFileList.add(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .join(fileOperator.generateCommittedFileName(TOPIC_PARTITION, 0, 1, ".txt"))
                        .build().path()
        );
        expectFileList.add(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .join(fileOperator.generateCommittedFileName(TOPIC_PARTITION, 0, 2, ".txt"))
                        .build().path()
        );
        expectFileList.add(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .join(fileOperator.generateCommittedFileName(TOPIC_PARTITION, 0, 3, ".txt"))
                        .build().path()
        );
        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();

        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();

        recordList.forEach(record -> tpWriter.buffer(record));
        tpWriter.write();

        // expect file name
        long containExpectFileCount = fileOperator.storage().list(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .build().path()
        ).stream().filter(file -> expectFileList.contains(file.getPath().toString())).count();
        assertEquals(3L, containExpectFileCount);

        // offset
        assertTrue(-1L != tpWriter.offset());

        // file count
        int fileCount = fileOperator.storage().list(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .build().path()
        ).size();
        assertEquals(15, fileCount);

        // close file count because rotation will create new file and close previous file.
        long fileSizeOfGreaterThanZeroCount = fileOperator.storage().list(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .build().path()
        ).stream().filter(file -> file.getLen() > 0).count();
//        assertEquals(14L, fileSizeOfGreaterThanZeroCount);
        assertEquals(14L, fileSizeOfGreaterThanZeroCount);
        tpWriter.commit();

        // commit the last file can't trigger rotation,because no new messages arrived
        fileSizeOfGreaterThanZeroCount = fileOperator.storage().list(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .build().path()
        ).stream().filter(file -> file.getLen() > 0).count();
//        assertEquals(14L, fileSizeOfGreaterThanZeroCount);
        assertEquals(15L, fileSizeOfGreaterThanZeroCount);

        // close
        tpWriter.close();
        // offset +1
        assertEquals(5L, tpWriter.offset());

        fileSizeOfGreaterThanZeroCount = fileOperator.storage().list(
                FilePath.of(storeConfig.tablePath())
                        .join(partition)
                        .build().path()
        ).stream().filter(file -> file.getLen() > 0).count();
        assertEquals(15L, fileSizeOfGreaterThanZeroCount);
        System.out.println();
    }
}
