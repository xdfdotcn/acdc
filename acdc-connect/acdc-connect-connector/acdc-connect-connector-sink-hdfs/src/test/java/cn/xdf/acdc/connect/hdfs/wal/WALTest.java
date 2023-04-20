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

import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WALTest extends TestWithMiniDFSCluster {

    private static final String EXTENSION = ".avro";

    private HdfsStorage storage;

    private boolean closed;

    @Test
    public void testMultiWALFromOneDFSClient() throws Exception {
        setUp();
        StoreConfig storeConfig = defaultStoreContext.getStoreConfig();
        FileUtils.jointPath(
                storeConfig.tablePath(),
                String.valueOf(TOPIC_PARTITION.partition())
        );
        fs.delete(new Path(
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        String.valueOf(TOPIC_PARTITION.partition())
                )
        ), true);

        @SuppressWarnings("unchecked")
        Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>) connectorConfig
                .getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
        storage = new HdfsStorage(connectorConfig, url);
        final WAL wal1 = storage.wal(storeConfig, TOPIC_PARTITION);
        final FSWAL wal2 = (FSWAL) storage.wal(storeConfig, TOPIC_PARTITION);

        String directory = storeConfig.table() + "/" + String.valueOf(PARTITION);
        final String tempFile =
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        directory,
                        defaultStoreContext.getFileOperator().generateTempFileName(extension)
                );
        final String committedFile =
                FileUtils.jointPath(
                        storeConfig.tablePath(),
                        directory,
                        defaultStoreContext.getFileOperator().generateCommittedFileName(TOPIC_PARTITION, 0, 10, extension)
                );
        fs.createNewFile(new Path(tempFile));
        wal1.acquireLease();
        wal1.append(WAL.beginMarker, "");
        wal1.append(tempFile, committedFile);
        wal1.append(WAL.endMarker, "");

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // holding the lease for time that is less than wal2's initial retry interval, which is 1000 ms.
                    Thread.sleep(WALConstants.INITIAL_SLEEP_INTERVAL_MS - 100);
                    closed = true;
                    wal1.close();
                } catch (ConnectException | InterruptedException e) {
                    // Ignored
                }
            }
        });
        thread.start();

        // acquireLease() will try to acquire the lease that wal1 is holding and fail. It will retry after 1000 ms.
        wal2.acquireLease();
        assertTrue(closed);
        wal2.apply();
        wal2.close();

        assertTrue(fs.exists(new Path(committedFile)));
        assertFalse(fs.exists(new Path(tempFile)));
        storage.close();
    }
}
