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
import cn.xdf.acdc.connect.hdfs.HdfsSinkConnectorTestBase;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.format.Format;
import cn.xdf.acdc.connect.hdfs.partitioner.PartitionerConfig;
import cn.xdf.acdc.connect.hdfs.utils.FileUtils;
import com.google.common.base.Strings;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class DefaultStoreConfigFactoryTest extends HdfsSinkConnectorTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        // store mode
        props.put(HdfsSinkConfig.STORAGE_FORMAT,
            Format.TEXT.name());
        props.put(HdfsSinkConfig.STORAGE_ROOT_PATH, StoreConstants.HDFS_ROOT);
        props.put(HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR, ",");
        // partitioner
        props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner");
        props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'dt'=yyyMMdd");
        props.put(PartitionerConfig.TIMEZONE_CONFIG, "Asia/Shanghai");
        props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "1000");
        props.put(HdfsSinkConfig.HDFS_URL_CONFIG, "hdfs://localhost:9001");
        props.put(StorageCommonConfig.STORE_URL_CONFIG, "hdfs://localhost:9001");
        return props;
    }

    private StoreConfig createExpectStoreConfig() {
        String textSeparator = connectorConfig.getString(HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR);
        String separator = Strings.isNullOrEmpty(textSeparator)
            ? HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR_DEFAULT
            : textSeparator;
        return StoreConfig.builder()
            .storeRootPath(FileUtils.jointPath(
                connectorConfig.url(), StoreConstants.HDFS_ROOT
            ))
            .walLogPath(FileUtils.jointPath(
                connectorConfig.url(),
                StoreConstants.HDFS_ROOT,
                HdfsSinkConstants.WAL_LOG_DIRECTORY
            ))
            .tempTablePath(FileUtils.jointPath(
                connectorConfig.url(),
                StoreConstants.HDFS_ROOT,
                HdfsSinkConstants.TEMPFILE_DIRECTORY,
                StoreConstants.HIVE_DB,
                StoreConstants.HIVE_TABLE
            ))
            .tablePath(FileUtils.jointPath(
                connectorConfig.url(),
                StoreConstants.HDFS_ROOT,
                HdfsSinkConstants.DATA_DIRECTORY,
                StoreConstants.HIVE_DB,
                StoreConstants.HIVE_TABLE
            ))
            .storeUrl(connectorConfig.url())
            .database(StoreConstants.HIVE_DB)
            .table(StoreConstants.HIVE_TABLE)
            .txtSeparator(separator)
            .format(Format.TEXT.name())
            .build();
    }

    @Test
    public void testCreateStoreConfigWithSpecifiedSeparator() {
        StoreConfig expectStoreConfig = createExpectStoreConfig();
        DefaultStoreConfigFactory defaultStoreConfigFactory =
            new DefaultStoreConfigFactory(connectorConfig);
        StoreConfig storeConfig = defaultStoreConfigFactory.createStoreConfig();
        assertEquals(expectStoreConfig.toString(), storeConfig.toString());
    }
}
