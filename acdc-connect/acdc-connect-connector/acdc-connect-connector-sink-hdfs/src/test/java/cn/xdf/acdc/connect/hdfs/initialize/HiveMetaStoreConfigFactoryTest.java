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

package cn.xdf.acdc.connect.hdfs.initialize;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveTable;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import com.google.common.base.Strings;
import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 {@link HiveMetaStoreConfigFactory}.
 */
public class HiveMetaStoreConfigFactoryTest extends HiveTestBase {

    private HiveMetaStore hiveMetaStore;

    private Partitioner partitioner;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.hiveMetaStore = defaultStoreContext.getHiveMetaStore();
        this.partitioner = defaultStoreContext.getPartitioner();
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        // store mode
        props.put(HdfsSinkConfig.STORAGE_ROOT_PATH, StoreConstants.HDFS_ROOT);
        // partitioner
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        return props;
    }

    private StoreConfig createExpectStoreConfig(final String txtSeparator) throws HiveException {
        String separator = Strings.isNullOrEmpty(txtSeparator)
            ? HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR_DEFAULT
            : txtSeparator;
        return StoreConfig.builder().storeRootPath(FileUtils.jointPath(url, StoreConstants.HDFS_ROOT))
            .walLogPath(FileUtils.jointPath(
                url,
                StoreConstants.HDFS_ROOT,
                HdfsSinkConstants.WAL_LOG_DIRECTORY
            ))
            .tempTablePath(FileUtils.jointPath(
                url,
                StoreConstants.HDFS_ROOT,
                HdfsSinkConstants.TEMPFILE_DIRECTORY,
                StoreConstants.HIVE_DB,
                StoreConstants.HIVE_TABLE
            ))
            .tablePath(FileUtils.jointPath(
                url,
                StoreConstants.HDFS_ROOT,
                StoreConstants.HIVE_DB,
                StoreConstants.HIVE_TABLE
            ))
            .storeUrl(url)
            .database(StoreConstants.HIVE_DB)
            .table(StoreConstants.HIVE_TABLE)
            .txtSeparator(separator)
            .format(Format.TEXT.name())
            .build();
    }

    @Test
    public void testCreateStorageConfigWithSpecifiedSeparator() throws HiveException {
        Table table = new HiveTable().createTxtTable(
            url,
            createSchema(),
            partitioner,
            StoreConstants.HIVE_DB,
            StoreConstants.HIVE_TABLE,
            ","
        );
        hiveMetaStore.createTable(table);
        StoreConfig expectStoreConfig = createExpectStoreConfig(",");
        HiveMetaStoreConfigFactory hiveMetaStoreConfigFactory =
            new HiveMetaStoreConfigFactory(connectorConfig, hiveMetaStore);
        StoreConfig storeConfig = hiveMetaStoreConfigFactory.createStoreConfig();
        assertEquals(expectStoreConfig.toString(), storeConfig.toString());
        table = hiveMetaStore.getTable(StoreConstants.HIVE_DB, StoreConstants.HIVE_TABLE);
        table.getPartitionKeys().forEach(partition -> {
            assertEquals(partition.getName(), "dt");
            assertEquals(partition.getType(), "string");
        });
    }

    @Test
    public void testCreateStorageConfigWithDefaultSeparator() throws HiveException {
        Table table = new HiveTable().createTxtTable(
            url,
            createSchema(),
            partitioner,
            StoreConstants.HIVE_DB,
            StoreConstants.HIVE_TABLE,
            null
        );
        hiveMetaStore.createTable(table);
        StoreConfig expectStoreConfig = createExpectStoreConfig(null);
        HiveMetaStoreConfigFactory hiveMetaStoreConfigFactory =
            new HiveMetaStoreConfigFactory(connectorConfig, hiveMetaStore);
        StoreConfig storeConfig = hiveMetaStoreConfigFactory.createStoreConfig();
        assertEquals(expectStoreConfig.toString(), storeConfig.toString());
        table = hiveMetaStore.getTable(StoreConstants.HIVE_DB, StoreConstants.HIVE_TABLE);
        table.getPartitionKeys().forEach(partition -> {
            assertEquals(partition.getName(), "dt");
            assertEquals(partition.getType(), "string");
        });
    }
}
