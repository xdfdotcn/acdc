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
import cn.xdf.acdc.connect.hdfs.TestWithMiniDFSCluster;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.initialize.StorageMode;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerFactory;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;
import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HiveTextTestBase extends TestWithMiniDFSCluster {
    // CHECKSTYLE:OFF

    protected HiveMetaStore hiveMetaStore;

    protected HiveExec hiveExec;

    protected Partitioner partitioner;

    protected StoreContext hiveMetaDataStoreContext;
    // CHECKSTYLE:ON

    private HdfsSinkConfig hdfsSinkConfig;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        return props;
    }


    /**
     * set up.
     * @Before should be omitted in order to be able to add properties per test.
     * @throws Exception set up fail.
     */
    public void setUp() throws Exception {
        super.setUp();
        Map<String, String> props = createProps();
        // hive integration mode
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE,
            HiveIntegrationMode.WITH_HIVE_META_DATA.name());
        props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "src/test/resources/conf");
        props.put(HdfsSinkConfig.STORAGE_MODE, StorageMode.AT_LEAST_ONCE.name());
        // store mode
        props.put(HdfsSinkConfig.STORAGE_FORMAT,
            Format.TEXT.name());
        props.put(HdfsSinkConfig.STORAGE_ROOT_PATH, StoreConstants.HDFS_ROOT);
        // partitioner
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");

        hdfsSinkConfig = new HdfsSinkConfig(props);
        hiveMetaStore = new HiveMetaStore(conf, hdfsSinkConfig);
        hiveExec = new HiveExec(hdfsSinkConfig);
        cleanHive();
        createDefaultTable();
        hiveMetaDataStoreContext = StoreContext.buildContext(hdfsSinkConfig);
    }

    protected void createDefaultTable() throws HiveException {
        Partitioner partitioner = PartitionerFactory.createPartitioner(hdfsSinkConfig);
        Table table = new HiveTable().createTxtTable(
            url,
            createPromotableSchema(),
            partitioner,
            StoreConstants.HIVE_DB,
            StoreConstants.HIVE_TABLE,
            null
        );
        hiveMetaStore.createTable(table);
    }

    /**
     * clean.
     * @throws Exception clean fail
     */
    public void tearDown() throws Exception {
//        cleanHive();
        super.tearDown();
    }

    protected void cleanHive() {
        // ensures all tables are removed
        for (String database : hiveMetaStore.getAllDatabases()) {
            if ("default".equals(database)) {
                for (String table : hiveMetaStore.getAllTables(database)) {
                    hiveMetaStore.dropTable(database, table);
                }
            }
        }
    }
}
