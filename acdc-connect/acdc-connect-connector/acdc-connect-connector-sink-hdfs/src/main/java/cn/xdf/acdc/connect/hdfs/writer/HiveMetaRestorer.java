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

package cn.xdf.acdc.connect.hdfs.writer;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.format.SchemaReader;
import cn.xdf.acdc.connect.hdfs.format.TableSchemaAndDataStatus;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.HiveIntegrationMode;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

@Slf4j
public class HiveMetaRestorer {

    private final Set<String> syncedPartitions;

    private final Partitioner partitioner;

    private final HiveIntegrationMode hiveIntegrationMode;

    private final SchemaReader schemaReader;

    private final StoreConfig storeConfig;

    private final HiveMetaStore hiveMetaStore;

    private final HdfsSinkConfig hdfsSinkConfig;

    private final String table;

    public HiveMetaRestorer(
            final HdfsSinkConfig hdfsSinkConfig,
            final StoreConfig storeConfig,
            final HiveMetaStore hiveMetaStore,
            final SchemaReader schemaReader,
            final Partitioner partitioner
    ) {
        this.hdfsSinkConfig = hdfsSinkConfig;
        this.storeConfig = storeConfig;
        this.hiveMetaStore = hiveMetaStore;
        this.schemaReader = schemaReader;
        this.hiveIntegrationMode = HiveIntegrationMode
                .valueOf(hdfsSinkConfig.getString(HdfsSinkConfig.HIVE_INTEGRATION_MODE));
        this.partitioner = partitioner;
        this.syncedPartitions = new HashSet<>();
        this.table = new StringBuilder()
                .append(storeConfig.database())
                .append(HdfsSinkConstants.DB_SEPARATOR)
                .append(storeConfig.table())
                .toString();
    }

    /**
     * Repair hive table meta data partition.
     *
     * @param partition partition path
     */
    public void addPartitionIfAbsent(final String partition) {
        if (!hiveIntegrationMode.isIntegrationHive()) {
            return;
        }
        HiveUtil hiveUtil = schemaReader
                .getHiveFactory()
                .createHiveUtil(
                        storeConfig,
                        hdfsSinkConfig,
                        hiveMetaStore
                );

        if (syncedPartitions.contains(partition)) {
            return;
        }

        syncedPartitions.add(partition);
        hiveUtil.addPartition(partition);
    }

    /**
     * Repair hive table schema.
     *
     * @param newestSchema The current newest kafka record schema
     */
    public void repairHiveTable(final Schema newestSchema) {
        if (!hiveIntegrationMode.isIntegrationHive()) {
            return;
        }
        try {
            HiveUtil hiveUtil = schemaReader
                    .getHiveFactory()
                    .createHiveUtil(
                            storeConfig,
                            hdfsSinkConfig,
                            hiveMetaStore
                    );
            hiveUtil.createTable(newestSchema, partitioner);
            hiveUtil.alterSchema(newestSchema);
            log.info("Repair table complete, table: {}, partition fields: {},newest schema: {} ",
                    table, partitioner.partitionFields(), newestSchema);
        } catch (HiveMetaStoreException e) {
            log.error("Repair table exception, exception is {} ", e.getMessage(), e);
            throw new ConnectException(e);
        }
    }

    /**
     * Repair hive table schema.
     *
     * @param topicPartition topic partition
     * @throws ConnectException Exception on sync hive
     */
    public void syncHiveMetaData(final TopicPartition topicPartition) throws ConnectException {
        if (!hiveIntegrationMode.isIntegrationHive()) {
            return;
        }
        try {
            HiveUtil hiveUtil = schemaReader
                    .getHiveFactory()
                    .createHiveUtil(
                            storeConfig,
                            hdfsSinkConfig,
                            hiveMetaStore
                    );
            TableSchemaAndDataStatus tableSchemaAndDataStatus = schemaReader.getTableSchemaAndDataStatus(topicPartition);
            if (tableSchemaAndDataStatus.isExistData()) {
                long start = System.currentTimeMillis();
                hiveUtil.createTable(tableSchemaAndDataStatus.getSchema(), partitioner);
                List<String> partitions = hiveUtil.listPartitions();
                for (String partition : tableSchemaAndDataStatus.getDataPartitions()) {
                    if (!partitions.contains(partition)) {
                        hiveUtil.addPartition(partition);
                    }
                }

                log.info("Sync hive table complete, "
                                + "table: {}, "
                                + "schema: {}, "
                                + "partition fields: {}, "
                                + "sync partition count:{},"
                                + " cost: {}",
                        table,
                        tableSchemaAndDataStatus.getSchema(),
                        partitioner.partitionFields(),
                        tableSchemaAndDataStatus.getDataPartitions().size(),
                        System.currentTimeMillis() - start
                );
            }
        } catch (HiveMetaStoreException e) {
            log.error("Sync hive table exception, exception is {} ", e.getMessage(), e);
            throw new ConnectException(e);
        }
    }
}
