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
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerFactory;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.After;

import java.util.Map;

public class HiveTestBase extends TestWithMiniDFSCluster {

    // CHECKSTYLE:OFF
    protected HiveMetaStore hiveMetaStore;

    protected HiveExec hiveExec;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "src/test/resources/conf");
        props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        props.put(HdfsSinkConfig.HIVE_INTEGRATION_MODE,
            HiveIntegrationMode.AUTO_CREATE_EXTERNAL_TABLE.name());
        return props;
    }

    protected void createDefaultTable() throws HiveException {
        Partitioner partitioner = PartitionerFactory.createPartitioner(connectorConfig);
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
     * should be omitted in order to be able to add properties per test.
     * @throws Exception exception on set up
     */
    public void setUp() throws Exception {
        super.setUp();
        hiveMetaStore = defaultStoreContext.getHiveMetaStore();
        hiveExec = new HiveExec(connectorConfig);
        cleanHive();
    }

    @After
    public void tearDown() throws Exception {
        cleanHive();
        super.tearDown();
    }

    protected String partitionLocation(int partition) {
        return partitionLocation(partition, "partition");
    }

    protected String partitionLocation(
        int partition,
        String partitionField) {
        String directory = defaultStoreContext.getStoreConfig().tablePath() + "/" + partitionField + "=" + partition;
        return directory;
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
