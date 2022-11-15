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

import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

/**
 * Utility class for integration with Hive.
 */
public abstract class HiveUtil {

    private final HiveMetaStore hiveMetaStore;

    private final StoreConfig storeConfig;

    private final AbstractConfig sinkConfig;

    public HiveUtil(
        final StoreConfig storeConfig,
        final AbstractConfig sinkConfig,
        final HiveMetaStore hiveMetaStore) {
        this.storeConfig = storeConfig;
        this.hiveMetaStore = hiveMetaStore;
        this.sinkConfig = sinkConfig;
    }

    /**
     * Create table by schema and partitioner.
     * @param schema schema
     * @param partitioner partitioner
     */
    public abstract void createTable(Schema schema, Partitioner<FieldSchema> partitioner);

    /**
     * Alter table by schema.
     * @param schema schema
     */
    public abstract void alterSchema(Schema schema);

    /**
     * Create a empty table structure.
     * @return empty table structure
     */
    public Table newTable() {
        return new Table(storeConfig.database(), hiveMetaStore.tableNameConverter(storeConfig.table()));
    }

    /**
     * Add partitions in metadata.
     * @param encodedPartition  encodedPartition
     */
    public void addPartition(final String encodedPartition) {
        hiveMetaStore.addPartition(storeConfig.database(), storeConfig.table(), encodedPartition);
    }

    /**
     * Get table all partitions in metadata.
     * @return all partitions in metadata
     */
    public List<String> listPartitions() {
        return hiveMetaStore.listPartitions(storeConfig.database(), storeConfig.table(), (short) -1);
    }

    protected HiveMetaStore getHiveMetaStore() {
        return hiveMetaStore;
    }

    protected StoreConfig getStoreConfig() {
        return storeConfig;
    }
}
