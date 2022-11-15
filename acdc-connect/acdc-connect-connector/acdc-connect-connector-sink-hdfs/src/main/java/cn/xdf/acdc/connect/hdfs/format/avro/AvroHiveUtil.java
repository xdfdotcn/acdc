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
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.SchemaConverter;
import cn.xdf.acdc.connect.hdfs.hive.HiveUtil;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.avro.AvroData;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class AvroHiveUtil extends HiveUtil {

    private static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

    private static final String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro"
        + ".AvroContainerInputFormat";

    private static final String AVRO_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro"
        + ".AvroContainerOutputFormat";

    private static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";

    private final AvroData avroData;

    private final HdfsSinkConfig config;

    private final HdfsStorage storage;

    private final StoreConfig storeConfig;

    public AvroHiveUtil(
        final StoreConfig storeConfig,
        final HdfsSinkConfig conf,
        final HiveMetaStore hiveMetaStore,
        final HdfsStorage storage
    ) {
        super(storeConfig, conf, hiveMetaStore);
        this.storage = storage;
        this.avroData = new AvroData(((HdfsStorage) storage).conf().avroDataConfig());
        this.config = conf;
        this.storeConfig = storeConfig;

    }

    @Override
    public void createTable(final Schema schema, final Partitioner partitioner)
        throws HiveMetaStoreException {
        Table table = constructAvroTable(schema, partitioner);
        getHiveMetaStore().createTable(table);
    }

    @Override
    public void alterSchema(final Schema schema) throws HiveMetaStoreException {
        Table table = getHiveMetaStore().getTable(storeConfig.database(), storeConfig.table());
        Schema filteredSchema = excludePartitionFieldsFromSchema(schema, table.getPartitionKeys());
        table.getParameters().put(AVRO_SCHEMA_LITERAL,
            avroData.fromConnectSchema(filteredSchema).toString());
        getHiveMetaStore().alterTable(table);
    }

    private Table constructAvroTable(
        final Schema schema,
        final Partitioner partitioner
    )
        throws HiveMetaStoreException {
        Table table = newTable();
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        // tableName is always the topic name
        String tablePath = this.storeConfig.tablePath();
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(AVRO_SERDE);
        try {
            table.setInputFormatClass(AVRO_INPUT_FORMAT);
            table.setOutputFormatClass(AVRO_OUTPUT_FORMAT);
        } catch (HiveException e) {
            throw new HiveMetaStoreException("Cannot find input/output format:", e);
        }
        Schema filteredSchema = excludePartitionFieldsFromSchema(schema, partitioner.partitionFields());
        List<FieldSchema> columns = SchemaConverter.convertSchema(filteredSchema);
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        table.getParameters().put(AVRO_SCHEMA_LITERAL,
            avroData.fromConnectSchema(filteredSchema).toString());
        return table;
    }

    /**
     * Remove the column(s) that is later re-created by Hive when using the
     * {@code partition.field.name} config.
     *
     * @param originalSchema the old schema to remove fields from
     * @param partitionFields the fields used for partitioning
     * @return the new schema without the fields used for partitioning
     */
    private Schema excludePartitionFieldsFromSchema(
        final Schema originalSchema,
        final List<FieldSchema> partitionFields
    ) {
        Set<String> partitions = partitionFields.stream()
            .map(FieldSchema::getName).collect(Collectors.toSet());

        SchemaBuilder newSchema = SchemaBuilder.struct();
        for (Field field : originalSchema.fields()) {
            if (!partitions.contains(field.name())) {
                newSchema.field(field.name(), field.schema());
            }
        }
        return newSchema;
    }
}
