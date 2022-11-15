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

package cn.xdf.acdc.connect.hdfs.format.parquet;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import org.apache.kafka.common.config.AbstractConfig;

public class ParquetHiveFactory implements HiveFactory {

    @Override
    public HiveUtil createHiveUtil(
        final StoreConfig storeConfig,
        final AbstractConfig config,
        final HiveMetaStore hiveMetaStore
    ) {
        return createHiveUtil(storeConfig, (HdfsSinkConfig) config, (HiveMetaStore) hiveMetaStore);
    }

    private HiveUtil createHiveUtil(
        final StoreConfig storeConfig,
        final HdfsSinkConfig config,
        final HiveMetaStore hiveMetaStore) {
        return new ParquetHiveUtil(storeConfig, config, hiveMetaStore);
    }
}
