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

package cn.xdf.acdc.connect.hdfs.format.orc;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.SchemaConverter;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class OrcHiveUtil extends HiveUtil {

    public OrcHiveUtil(
        final StoreConfig storeConfig,
        final HdfsSinkConfig config,
        final HiveMetaStore hiveMetaStore) {
        super(storeConfig, config, hiveMetaStore);
    }

    @Override
    public void alterSchema(final Schema schema) {
        Table table = getHiveMetaStore().getTable(getStoreConfig().database(), getStoreConfig().table());
        List<FieldSchema> columns = SchemaConverter.convertSchema(schema);
        table.setFields(columns);
        getHiveMetaStore().alterTable(table);
    }

    @Override
    public void createTable(
        final Schema schema,
        final Partitioner<FieldSchema> partitioner
    ) throws HiveMetaStoreException {
        Table table = constructOrcTable(schema, partitioner);
        getHiveMetaStore().createTable(table);
    }

    private Table constructOrcTable(
        final Schema schema,
        final Partitioner<FieldSchema> partitioner
    ) throws HiveMetaStoreException {

        Table table = newTable();
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");

        // tableName is always the topic name
        String tablePath = getStoreConfig().tablePath();
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(getHiveOrcSerde());

        try {
            table.setInputFormatClass(getHiveOrcInputFormat());
            table.setOutputFormatClass(getHiveOrcOutputFormat());
        } catch (HiveException e) {
            throw new HiveMetaStoreException("Cannot find input/output format:", e);
        }

        // convert Connect schema schema to Hive columns
        List<FieldSchema> columns = SchemaConverter.convertSchema(schema);
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        return table;
    }

    private String getHiveOrcInputFormat() {
        return OrcInputFormat.class.getName();
    }

    private String getHiveOrcOutputFormat() {
        return OrcOutputFormat.class.getName();
    }

    private String getHiveOrcSerde() {
        return OrcSerde.class.getName();
    }

}
