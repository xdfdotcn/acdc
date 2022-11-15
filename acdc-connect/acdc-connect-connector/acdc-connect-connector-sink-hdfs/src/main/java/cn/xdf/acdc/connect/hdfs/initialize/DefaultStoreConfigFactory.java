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
import cn.xdf.acdc.connect.hdfs.storage.FilePath;

public class DefaultStoreConfigFactory implements StoreConfigFactory {

    private HdfsSinkConfig config;

    public DefaultStoreConfigFactory(final HdfsSinkConfig config) {
        this.config = config;
    }

    @Override
    public StoreConfig createStoreConfig() {
        String dataBase = config.database();
        String table = config.table();
        String url = config.url();
        String rootDir = config.getString(HdfsSinkConfig.STORAGE_ROOT_PATH);
        return StoreConfig.builder()
            .storeRootPath(FilePath.of(url)
                .join(config.getString(HdfsSinkConfig.STORAGE_ROOT_PATH))
                .build().path()

            )
            .walLogPath(FilePath.of(url)
                .join(rootDir)
                .join(HdfsSinkConstants.WAL_LOG_DIRECTORY)
                .build().path()
            )
            .tmpTablePath(FilePath.of(url)
                .join(rootDir)
                .join(HdfsSinkConstants.TEMPFILE_DIRECTORY)
                .join(dataBase)
                .join(table)
                .build().path()
            )
            .tablePath(FilePath.of(url)
                .join(rootDir)
                .join(HdfsSinkConstants.DATA_DIRECTORY)
                .join(dataBase)
                .join(table)
                .build().path()
            )
            .storeUrl(url)
            .database(dataBase)
            .table(table)
            .format(config.getString(HdfsSinkConfig.STORAGE_FORMAT))
            .txtSeparator(config.getString(HdfsSinkConfig.STORAGE_FORMAT_TEXT_SEPARATOR))
            .build();
    }
}
