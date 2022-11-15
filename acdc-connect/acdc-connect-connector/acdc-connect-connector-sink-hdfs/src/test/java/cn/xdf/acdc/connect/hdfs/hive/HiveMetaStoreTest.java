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

package cn.xdf.acdc.connect.hdfs.hive;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.common.Schemas;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class HiveMetaStoreTest extends HiveTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        props.put(HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR, ",");
        return props;
    }

    @Test
    public void testAccessMetaStoreShouldSuccessWithSynchronousWhenMultithreading() throws HiveException, InterruptedException, IOException {
        final HiveMetaStore hiveMetaStore = new HiveMetaStore(connectorConfig);
        final StoreContext storeContext = StoreContext.buildContext(connectorConfig, hiveMetaStore);
        final Partitioner partitioner = storeContext.getPartitioner();
        final StoreConfig storeConfig = storeContext.getStoreConfig();

        final AtomicInteger runSuccessCounter = new AtomicInteger();
        final int runTimes = 50;
        final int threadCount = 3;
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);
        final CountDownLatch cl = new CountDownLatch(threadCount);
        final ExecutorService executorService = Executors.newFixedThreadPool(10);

        final List<FieldSchema> fieldSchemaList = Schemas.createHivePrimitiveSchemaWithAllFieldType();
        final Table table = new HiveTable().createTable(url, fieldSchemaList, partitioner, storeConfig.textSeparator());

        // create table
        hiveMetaStore.createTable(table);

        // Thread 1
        executorService.submit(new MetaStoreAccessTask(cyclicBarrier, hiveMetaStore, storeConfig, runTimes, runSuccessCounter, cl));
        // Thread 2
        executorService.submit(new MetaStoreAccessTask(cyclicBarrier, hiveMetaStore, storeConfig, runTimes, runSuccessCounter, cl));
        // Thread 3
        executorService.submit(new MetaStoreAccessTask(cyclicBarrier, hiveMetaStore, storeConfig, runTimes, runSuccessCounter, cl));

        cl.await();
        assertTrue(runSuccessCounter.get() == runTimes * threadCount);
    }
}
