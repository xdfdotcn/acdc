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
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.storage.FilePath;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HiveMetaStoreConfigFactory implements StoreConfigFactory {

    private HiveMetaStore hiveMetaStore;

    private HdfsSinkConfig config;

    public HiveMetaStoreConfigFactory(
        final HdfsSinkConfig config,
        final HiveMetaStore hiveMetaStore) {
        this.config = config;
        this.hiveMetaStore = hiveMetaStore;
    }

    private String formatClass(final Table table) {
        String className = table.getOutputFormatClass().getName();
        return className;
    }

    @Override
    public StoreConfig createStoreConfig() {
        // config table name eg:"cdc.sink_city"
        String dataBase = config.database();
        String tableName = config.table();
        String url = config.url();
        Table table = hiveMetaStore.getTable(dataBase, tableName);
        StorageDescriptor storageDescriptor = table.getSd();
        String location = storageDescriptor.getLocation();
        String txtSeparator = storageDescriptor.getSerdeInfo().getParameters().get(StoreConfig.TXT_SEPARATOR);
        String rootPath = config.getString(HdfsSinkConfig.STORAGE_ROOT_PATH);
        return StoreConfig.builder()
            .storeRootPath(FilePath.of(url)
                .join(rootPath)
                .build().path()
            )
            .walLogPath(FilePath.of(url)
                .join(rootPath)
                .join(HdfsSinkConstants.WAL_LOG_DIRECTORY)
                .build().path()

            )
            .tmpTablePath(FilePath.of(url)
                .join(rootPath)
                .join(HdfsSinkConstants.TEMPFILE_DIRECTORY)
                .join(dataBase)
                .join(tableName)
                .build().path()
            )
            .tablePath(location)
            .storeUrl(url)
            .database(dataBase)
            .table(tableName)
            .txtSeparator(txtSeparator)
            .classNameOfFormat(formatClass(table))
            .build();
    }
}
