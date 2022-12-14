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

package cn.xdf.acdc.connect.hdfs.storage;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.initialize.HiveMetaStoreConfigFactory;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 {@link HdfsFileOperator}.
 */
public class HdfsFileOperatorTest extends HiveTestBase {

    private HdfsStorage hdfsStorage;

    private HdfsFileOperator operator;

    private StoreConfig storeConfig;

    private String zeroPadFormat;

    private String extension;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.createDefaultTable();
        this.hdfsStorage = new HdfsStorage(connectorConfig, url);
        initComponent();
    }

    private void initComponent() {
        this.storeConfig = new HiveMetaStoreConfigFactory(connectorConfig, hiveMetaStore)
            .createStoreConfig();
        this.operator = new HdfsFileOperator(
            hdfsStorage,
            storeConfig,
            connectorConfig
        );
        this.zeroPadFormat = "%0"
            + this.connectorConfig.getInt(HdfsSinkConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
            + "d";
        this.extension = ".txt";
    }

    @Test
    public void testCreateCommitFileByPartitionAndTp() throws IOException {
        String encodePartition = "dt=20210714";
        long startVer = 1000;
        long endVer = 20000;
        String fileName = new StringBuilder()
            .append(storeConfig.table())
            .append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR)
            .append(TOPIC_PARTITION.partition())
            .append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR)
            .append(String.format(zeroPadFormat, startVer))
            .append(HdfsSinkConstants.COMMMITTED_FILENAME_SEPARATOR)
            .append(String.format(zeroPadFormat, endVer))
            .append(extension).toString();
        String expectCommitFile = FilePath.of(storeConfig.tablePath())
            .join(encodePartition)
            .join(fileName).build().path();

        String actualCommitFile = operator.createCommitFileByPartitionAndTp(
            encodePartition,
            TOPIC_PARTITION,
            startVer,
            endVer,
            extension
        );
        System.out.println(expectCommitFile);
        assertEquals(expectCommitFile, actualCommitFile);
        hdfsStorage.createFile(actualCommitFile);
        assertTrue(hdfsStorage.exists(actualCommitFile));
    }

    @Test
    public void createTempFileByPartition() throws IOException {
        String encodePartition = "dt=20210714";
        String tempFile = operator.createTempFileByPartition(
            encodePartition,
            extension
        );
        System.out.println(tempFile);
        hdfsStorage.createFile(tempFile);
        assertTrue(hdfsStorage.exists(tempFile));
    }

    @Test
    public void testFindMaxVerFileByTpShouldNotExistFile() {
        Optional<FileStatus> fileStatus = operator.findMaxVerFileByTp(TOPIC_PARTITION);
        assertTrue(!fileStatus.isPresent());

        fileStatus = operator.findMaxVerFileByPartitionAndTp(TOPIC_PARTITION, "dt=20210714");
        assertTrue(!fileStatus.isPresent());

        fileStatus = operator.findTableMaxVerFile();
        assertTrue(!fileStatus.isPresent());
    }

    /**
     *  /unit_test/default/test_sink/dt=20210714/test_sink+12+0000000000+0000000010.txt.
     *  /unit_test/default/test_sink/dt=20210714/test_sink+12+0000000000+0000000011.txt.
     *  /unit_test/default/test_sink/dt=20210714/test_sink+13+0000000000+0000000013.txt
     *  /unit_test/default/test_sink/dt=20210714/test_sink+14+0000000000+0000000015.txt
     *  /unit_test/default/test_sink/dt=20210715/test_sink+12+0000000000+0000000012.txt
     *  /unit_test/default/test_sink/dt=20210715/test_sink+13+0000000000+0000000014.txt
     *  /unit_test/default/test_sink/dt=20210715/test_sink+14+0000000000+0000000016.txt
     */
    @Test
    public void testFindMaxVerFile() {
        String encodePartition1 = "dt=20210714";
        String encodePartition2 = "dt=20210715";
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition1,
                TOPIC_PARTITION,
                0,
                10,
                extension
            ));
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition1,
                TOPIC_PARTITION,
                0,
                11,
                extension
            ));

        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition2,
                TOPIC_PARTITION,
                0,
                12,
                extension
            ));
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition1,
                TOPIC_PARTITION2,
                0,
                13,
                extension
            ));
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition2,
                TOPIC_PARTITION2,
                0,
                14,
                extension
            ));
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition1,
                TOPIC_PARTITION3,
                0,
                15,
                extension
            )
        );
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition2,
                TOPIC_PARTITION3,
                0,
                16,
                extension
            )
        );

        System.out.println(operator.findMaxVerFileByTp(TOPIC_PARTITION).get().getPath().toString());
        System.out.println(operator.findMaxVerFileByTp(TOPIC_PARTITION).get().getPath().getName());
        System.out.println(operator.findMaxVerFileByTp(TOPIC_PARTITION).get().getPath());

        assertEquals(12, HdfsFileOperator.extractVersion(
            operator.findMaxVerFileByTp(TOPIC_PARTITION)
                .get()
                .getPath()
                .getName()));

        assertEquals(14, HdfsFileOperator.extractVersion(
            operator.findMaxVerFileByTp(TOPIC_PARTITION2)
                .get()
                .getPath()
                .getName()));

        assertEquals(16, HdfsFileOperator.extractVersion(
            operator.findMaxVerFileByTp(TOPIC_PARTITION3)
                .get()
                .getPath()
                .getName()));

        assertEquals(16, HdfsFileOperator.extractVersion(
            operator.findTableMaxVerFile().get().getPath().getName()
        ));

        assertEquals(11, HdfsFileOperator.extractVersion(
            operator.findMaxVerFileByPartitionAndTp(TOPIC_PARTITION, encodePartition1).get().getPath().getName()
        ));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractVersionShouldThrownExceptionWithNotMatches() {
        String encodePartition = "dt=20210714";
        hdfsStorage.createFile(
            operator.createCommitFileByPartitionAndTp(
                encodePartition,
                TOPIC_PARTITION3,
                0,
                16,
                extension
            )
        );
        assertEquals(16, HdfsFileOperator.extractVersion(
            operator.findMaxVerFileByTp(TOPIC_PARTITION3)
                .get()
                .getPath()
                .toString()));
    }

    /**
     * The same partition but different Tp.
     * expect: every Tp should create new File
     */
    @Test
    public void testCreateCommitFileByRotation() {
        String encodePartition = "dt=20210714";
        createCommitFileByRotation(
            encodePartition,
            TOPIC_PARTITION
        );
        createCommitFileByRotation(
            encodePartition,
            TOPIC_PARTITION2
        );
    }

    @Test
    public void testExtractVersion() {
        assertEquals(1001, HdfsFileOperator.extractVersion("namespace.topic+1+1000+1001.avro"));
        assertEquals(1001, HdfsFileOperator.extractVersion("namespace.topic+1+1000+1001"));
        assertEquals(1001, HdfsFileOperator.extractVersion("namespace-topic_stuff.foo+1+1000+1001.avro"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractVersionShouldThrownExceptionWithNotMatch() {
        assertEquals(1001, HdfsFileOperator.extractVersion("namespace+topic+1+1000+1001.avro"));
    }

    private void createCommitFileByRotation(
        final String encodePartition,
        final TopicPartition tp
    ) {
        String commitFile = operator.createCommitFileByRotation(
            encodePartition,
            tp,
            extension
        );
        System.out.println(commitFile);
        String[] arr = commitFile.split("/");
        long expectVer = 1;
        long actualVer = HdfsFileOperator.extractVersion(arr[arr.length - 1]);
        assertEquals(expectVer, actualVer);
        hdfsStorage.createFile(commitFile);
        assertTrue(hdfsStorage.exists(commitFile));

        commitFile = operator.createCommitFileByRotation(
            encodePartition,
            tp,
            extension
        );
        arr = commitFile.split("/");
        expectVer = 2;
        actualVer = HdfsFileOperator.extractVersion(arr[arr.length - 1]);
        assertEquals(expectVer, actualVer);
        hdfsStorage.createFile(commitFile);
        assertTrue(hdfsStorage.exists(commitFile));

        commitFile = operator.createCommitFileByRotation(
            encodePartition,
            tp,
            extension
        );
        arr = commitFile.split("/");
        expectVer = 3;
        actualVer = HdfsFileOperator.extractVersion(arr[arr.length - 1]);
        assertEquals(expectVer, actualVer);
    }
}
