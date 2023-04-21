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

package cn.xdf.acdc.connect.hdfs.wal;

import cn.xdf.acdc.connect.hdfs.HdfsWriterCoordinator;
import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FSWALTest extends TestWithMiniDFSCluster {

    @Test
    public void testTruncate() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = new FSWAL(storeConfig, tp, storage);
        String logFile = FileUtils.jointPath(
                storeConfig.walLogPath(),
                storeConfig.table(),
                String.valueOf(tp.partition()),
                FileUtils.logFileName());
        wal.append("a", "b");
        assertTrue("WAL file should exist after append",
                storage.exists(logFile));
        wal.truncate();
        assertFalse("WAL file should not exist after truncate",
                storage.exists(logFile));
        assertTrue("Rotated WAL file should exist after truncate",
                storage.exists(logFile + ".1"));
        wal.append("c", "d");
        assertTrue("WAL file should be recreated after truncate + append",
                storage.exists(logFile));
        assertTrue("Rotated WAL file should exist after truncate + append",
                storage.exists(logFile + ".1"));
    }

    @Test
    public void testEmptyWalFileRecovery() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        String logFile = FileUtils.jointPath(
                storeConfig.walLogPath(),
                storeConfig.table(),
                String.valueOf(tp.partition()),
                FileUtils.logFileName());

        fs.create(new Path(logFile), true);
        FSWAL wal = new FSWAL(storeConfig, tp, storage);
        wal.acquireLease();
    }

    @Test
    public void testTruncatedVersionWalFileRecovery() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        HdfsFileOperator fileOperator = defaultStoreContext.getFileOperator();
        TopicPartition tp = new TopicPartition("mytopic", 123);
        String logFile = FileUtils.jointPath(
                storeConfig.walLogPath(),
                storeConfig.table(),
                String.valueOf(tp.partition()),
                FileUtils.logFileName());

        OutputStream o = fs.create(new Path(logFile), true);
        o.write(47);
        o.write(61);
        FSWAL wal = new FSWAL(storeConfig, tp, storage);
        wal.acquireLease();
    }

    @Test
    public void testExtractOffsetsFromPath() {
        List<String> filepaths = Arrays.asList(
                "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0000000000+0000000000.avro",
                "hdfs://namenode:8020/topics/test_hdfs/f1=value6/test_hdfs+0+0000000005+0000000005.avro",
                "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0000000006+0000000009.avro",
                "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0000001034+0000001333.avro",
                "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0123132133+0213314343.avro"
        );
        long[] expectedOffsets = {0, 5, 9, 1333, 213314343};

        int index = 0;
        for (String path : filepaths) {
            long extractedOffset = FSWAL.extractOffsetsFromFilePath(path);
            assertEquals(expectedOffsets[index], extractedOffset);
            index++;
        }
    }

    @Test
    public void testOffsetsExtractedFromWALWithEmptyBlocks() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();

        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);
        //create a few empty blocks
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");

        assertNull(wal.extractLatestOffset());

        addSampleEntriesToWAL(wal, 5);
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");

        long latestOffset = wal.extractLatestOffset().getOffset();
        assertEquals(49, latestOffset);
    }

    @Test
    public void testNoOffsetsFromWALWithMissingEndMarkerFirstBlock() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        // test missing end marker on middle block
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);

        wal.append(WAL.beginMarker, "");
        addSampleEntriesToWALNoMarkers(wal, 5);
        // missing end marker here

        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.endMarker, "");
        wal.close();

        assertNull(wal.extractLatestOffset());
    }

    @Test
    public void testNoOffsetsFromWALWithMissingEndMarkerMiddleBlock() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        // test missing end marker on middle block
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);
        //create a few empty blocks
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");

        wal.append(WAL.beginMarker, "");
        addSampleEntriesToWALNoMarkers(wal, 5);
        // missing end marker here

        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.endMarker, "");
        wal.close();

        assertNull(wal.extractLatestOffset());
    }

    @Test
    public void testOffsetsFromWALWithMissingEndMarkerLastBlockAndValidPreviousBlock() throws Exception {
        long expectedOffset = 108L;
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        // test missing end marker on middle block
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);
        wal.append(WAL.beginMarker, "");

        String tempfile =
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        getEncodingPartition(PARTITION),
                        defaultStoreContext.getFileOperator().generateTempFileName(extension)
                );
        fs.createNewFile(new Path(tempfile));
        String committedFile =
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        getEncodingPartition(PARTITION),
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 9, expectedOffset, extension)
                );
        wal.append(tempfile, committedFile);

        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        addSampleEntriesToWALNoMarkers(wal, 5);
        wal.close();
        //missing end marker here
        assertEquals(expectedOffset, wal.extractLatestOffset().getOffset());
    }

    @Test
    public void testNoOffsetsFromWALWithMissingEndMarkerLastBlock() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        // test missing end marker on middle block
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        addSampleEntriesToWALNoMarkers(wal, 5);
        //missing end marker here
        assertNull(wal.extractLatestOffset());
    }

    @Test
    public void testNoOffsetsFromWALWithMissingBeginMarkerFirstBlock() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);

        //test missing begin marker
        addSampleEntriesToWALNoMarkers(wal, 5);
        wal.append(WAL.endMarker, "");
        wal.close();
        assertNull(wal.extractLatestOffset());
    }

    @Test
    public void testNoOffsetsFromWALWithMissingBeginMarkerMiddleBlock() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);

        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        //test missing begin marker
        addSampleEntriesToWALNoMarkers(wal, 5);
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.close();
        assertNull(wal.extractLatestOffset());
    }

    @Test
    public void testNoOffsetsFromWALWithMissingBeginMarkerLastBlock() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);

        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        wal.append(WAL.beginMarker, "");
        wal.append(WAL.endMarker, "");
        //test missing begin marker
        addSampleEntriesToWALNoMarkers(wal, 5);
        wal.append(WAL.endMarker, "");
        wal.close();
        assertNull(wal.extractLatestOffset());
    }

    @Test
    public void testOffsetsExtractedFromWAL() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);
        addSampleEntriesToWAL(wal, 5);

        long latestOffset = wal.extractLatestOffset().getOffset();
        assertEquals(49, latestOffset);
    }

    @Test
    public void testOffsetsExtractedFromOldWAL() throws Exception {
        setupWalTest();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FSWAL wal = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);
        addSampleEntriesToWAL(wal, 5);
        //creates old WAL and empties new one
        wal.truncate();

        long latestOffset = wal.extractLatestOffset().getOffset();
        assertEquals(49, latestOffset);
    }

    private void setupWalTest() throws Exception {
        setUp();
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        fs.delete(new Path(
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        String.valueOf(TOPIC_PARTITION.partition())
                )));
        HdfsWriterCoordinator hdfsWriter = new HdfsWriterCoordinator(connectorConfig, context);
        partitioner = hdfsWriter.getPartitioner();
    }

    private void addSampleEntriesToWAL(final WAL wal, int numEntries) throws IOException {
        wal.append(WAL.beginMarker, "");
        addSampleEntriesToWALNoMarkers(wal, numEntries);
        wal.append(WAL.endMarker, "");
        wal.close();
    }

    private void addSampleEntriesToWALNoMarkers(final WAL wal, int numEntries) throws IOException {
        for (int i = 0; i < numEntries; ++i) {
            long startOffset = i * 10;
            long endOffset = (i + 1) * 10 - 1;
            StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
            // CHECKSTYLE:OFF
            String tempfile = (
                    FileUtils.jointPath(
                            storeConfig.tablePath(),
                            getEncodingPartition(PARTITION),
                            defaultStoreContext.getFileOperator().generateTempFileName(extension)
                    ));
            fs.createNewFile(new Path(tempfile));
            String committedFile = FileUtils.jointPath(
                    storeConfig.tablePath(),
                    getEncodingPartition(PARTITION),
                    defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, startOffset, endOffset, extension)
            );
            wal.append(tempfile, committedFile);
        }
    }

    @Test
    public void testApply() throws Exception {
        setUp();
        System.out.println("Testing testApply...");
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        HdfsFileOperator fileOperator = defaultStoreContext.getFileOperator();
        TopicPartition tp = new TopicPartition("mytopic", 123);
        FSWAL wal = new FSWAL(storeConfig, tp, storage);
        String logFile = FileUtils.jointPath(
                storeConfig.walLogPath(),
                storeConfig.table(),
                String.valueOf(tp.partition()),
                FileUtils.logFileName());
        wal.append("a", "b");
        assertTrue("WAL file should exist after append",
                storage.exists(logFile));
        wal.apply();
        wal.append("x", "y");
        wal.apply();
    }
}
