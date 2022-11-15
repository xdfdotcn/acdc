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

package cn.xdf.acdc.connect.hdfs.format.avro;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import org.apache.kafka.common.config.AbstractConfig;

public class AvroHiveFactory implements HiveFactory {

    private final HdfsStorage storage;

    public AvroHiveFactory(final HdfsStorage storage) {
        this.storage = storage;
    }

    @Override
    public HiveUtil createHiveUtil(
        final StoreConfig storeConfig,
        final AbstractConfig conf,
        final HiveMetaStore hiveMetaStore
    ) {
        return new AvroHiveUtil(storeConfig, (HdfsSinkConfig) conf, hiveMetaStore, storage);
    }
}
