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

package cn.xdf.acdc.connect.hdfs.format.metadata;

import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemas;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

public class HiveMetaUtil extends HiveUtil {

    public HiveMetaUtil(
        final StoreConfig storeConfig,
        final AbstractConfig sinkConfig,
        final HiveMetaStore hiveMetaStore) {
        super(storeConfig, sinkConfig, hiveMetaStore);
    }

    @Override
    public void createTable(
        final Schema schema,
        final Partitioner<FieldSchema> partitioner) {
        // not support create table ,because table must be exist.
    }

    @Override
    public void alterSchema(final Schema schema) {
        Preconditions.checkNotNull(schema);

        Table table = getHiveMetaStore().getTable(getStoreConfig().database(), getStoreConfig().table());
        List<FieldSchema> columns = SinkSchemas.convertToFieldColumns(schema);
        table.setFields(columns);
        getHiveMetaStore().alterTable(table);
    }
}
