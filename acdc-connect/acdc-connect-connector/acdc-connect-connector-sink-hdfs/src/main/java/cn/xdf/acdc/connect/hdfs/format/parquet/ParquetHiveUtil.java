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
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.SchemaConverter;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ParquetHiveUtil extends HiveUtil {

    public ParquetHiveUtil(
        final StoreConfig storeConfig,
        final HdfsSinkConfig conf,
        final HiveMetaStore hiveMetaStore) {
        super(storeConfig, conf, hiveMetaStore);
    }

    @Override
    public void createTable(
        final Schema schema,
        final Partitioner partitioner
    ) throws HiveMetaStoreException {
        Table table = constructParquetTable(schema, partitioner);
        getHiveMetaStore().createTable(table);
    }

    @Override
    public void alterSchema(final Schema schema) {
        Table table = getHiveMetaStore().getTable(getStoreConfig().database(), getStoreConfig().table());
        List<FieldSchema> columns = SchemaConverter.convertSchema(schema);
        removeFieldPartitionColumn(columns, table.getPartitionKeys());
        table.setFields(columns);
        getHiveMetaStore().alterTable(table);
    }

    private Table constructParquetTable(
        final Schema schema,
        final Partitioner partitioner
    ) throws HiveMetaStoreException {
        Table table = newTable();
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        // tableName is always the topic name
        String tablePath = getStoreConfig().tablePath();
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(getHiveParquetSerde());
        try {
            table.setInputFormatClass(getHiveParquetInputFormat());
            table.setOutputFormatClass(getHiveParquetOutputFormat());
        } catch (HiveException e) {
            throw new HiveMetaStoreException("Cannot find input/output format:", e);
        }
        // convert Connect schema schema to Hive columns
        List<FieldSchema> columns = SchemaConverter.convertSchema(schema);
        removeFieldPartitionColumn(columns, partitioner.partitionFields());
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        return table;
    }

    private String getHiveParquetInputFormat() {
        String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
        String oldClass = "parquet.hive.DeprecatedParquetInputFormat";

        try {
            Class.forName(newClass);
            return newClass;
        } catch (ClassNotFoundException ex) {
            return oldClass;
        }
    }

    private String getHiveParquetOutputFormat() {
        String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
        String oldClass = "parquet.hive.DeprecatedParquetOutputFormat";

        try {
            Class.forName(newClass);
            return newClass;
        } catch (ClassNotFoundException ex) {
            return oldClass;
        }
    }

    private String getHiveParquetSerde() {
        String newClass = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        String oldClass = "parquet.hive.serde.ParquetHiveSerDe";

        try {
            Class.forName(newClass);
            return newClass;
        } catch (ClassNotFoundException ex) {
            return oldClass;
        }
    }

    /**
     * Remove the column that is later re-created by Hive when using the
     * {@code partition.field.name} config.
     *
     * @param columns the hive columns from
     *                {@link SchemaConverter#convertSchema(Schema) convertSchema}.
     * @param partitionFields the fields used for partitioning
     */
    private void removeFieldPartitionColumn(
        final List<FieldSchema> columns,
        final List<FieldSchema> partitionFields
    ) {
        Set<String> partitions = partitionFields.stream()
            .map(FieldSchema::getName).collect(Collectors.toSet());

        columns.removeIf(column -> partitions.contains(column.getName()));
    }
}
