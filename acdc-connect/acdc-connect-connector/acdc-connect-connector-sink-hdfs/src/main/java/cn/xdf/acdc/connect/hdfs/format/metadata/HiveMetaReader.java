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

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConfig;
import cn.xdf.acdc.connect.hdfs.format.AbstractSchemaFileReader;
import cn.xdf.acdc.connect.hdfs.format.ProjectedResult;
import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.errors.HiveMetaStoreException;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchema;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemas;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

@Slf4j
public class HiveMetaReader extends AbstractSchemaFileReader {

    private static final String SCHEMA_NAME = "hive_meta_data_schema";

    private static final String SCHEMA_UNIQUE_KEY_SEPARATOR = "-";

    private static final Schema NULL_SCHEMA = null;

    private final HiveMetaStore hiveMetaStore;

    private final StoreConfig storeConfig;

    private Schema currentSchema;

    private final boolean isSupportSchemaChange;

    private final SchemaProjector schemaProjector;

    public HiveMetaReader(
        final HdfsSinkConfig hdfsSinkConf,
        final StoreConfig storeConfig,
        final HdfsFileOperator fileOperator,
        final HiveMetaStore hiveMetaStore
    ) {
        super(hdfsSinkConf, storeConfig, fileOperator);
        this.storeConfig = storeConfig;
        this.hiveMetaStore = hiveMetaStore;
        this.isSupportSchemaChange = hdfsSinkConf.getBoolean(HdfsSinkConfig.HIVE_SCHEMA_CHANGE_SUPPORT);
        this.schemaProjector = new SchemaProjector();
    }

    private Schema readHiveTableSchema() {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
            .name(SCHEMA_NAME)
            .version(1);
        return readHiveTableSchema(schemaBuilder);
    }

    private Schema readHiveTableSchema(final SchemaBuilder schemaBuilder) {
        try {
            Table table = hiveMetaStore.getTable(storeConfig.database(), storeConfig.table());
            ObjectInspector objInspector = table.getDeserializer().getObjectInspector();
            Category category = objInspector.getCategory();
            if (Category.STRUCT != category) {
                throw new ConnectException(
                    "Top level type must be of type STRUCT, but was "
                        + objInspector.getCategory().name()
                );
            }

            StructObjectInspector structInspector = (StructObjectInspector) objInspector;
            List<StructField> structFieldList = (List<StructField>) structInspector.getAllStructFieldRefs();
            return SinkSchemas.convertToStructSchema(structFieldList, schemaBuilder);
        } catch (HiveMetaStoreException e) {
            throw new ConnectException(String.format("Read schema metastore exception for table %s.%s",
                storeConfig.database(),
                storeConfig.table()),
                e);
        } catch (SerDeException e) {
            throw new ConnectException(String.format("Read schema serialize exception for table %s.%s",
                storeConfig.database(),
                storeConfig.table()),
                e);
        } catch (DataException e) {
            throw new ConnectException(String.format("Read schema kafka data exception for table %s.%s",
                storeConfig.database(),
                storeConfig.table()),
                e);
        } catch (ConnectException e) {
            throw new ConnectException(String.format("Read schema kafka connect exception for table %s.%s",
                storeConfig.database(),
                storeConfig.table()),
                e);
        }
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * 1. 以sink端的db的schema为准，进行数据record投影
     * 2. 相对于sink端db的schema，record中存在字段缺失，默认返回null，后续可以考虑支持默认值
     * 3. 相对于sink端db的schema，record中的字段在sink端db的schema中存在缺失，则参照sink端schema的声明顺序,在末尾追加缺失字段
     * 4. 如果设置为不支持schema 变更，则忽略"3"中的处理
     */
    private SinkRecord project(final SinkRecord sinkRecord, final Schema targetSchema) {
        return schemaProjector.projectRecord(sinkRecord, null, targetSchema);
    }

    private String schemaUniqueKey(final Schema structSchema) {
        return new StringBuilder()
            .append(structSchema.name()).append(SCHEMA_UNIQUE_KEY_SEPARATOR)
            .append(structSchema.type()).append(SCHEMA_UNIQUE_KEY_SEPARATOR)
            .append(structSchema.version()).append(SCHEMA_UNIQUE_KEY_SEPARATOR)
            .append(structSchema.parameters())
            .toString();
    }

    private List<Field> searchMissingFieldFromSource(final Schema source, final Schema target) {
        List<Field> missingFields = new ArrayList<>(source.fields().size());
        String delLogicalFieldName = SinkConfig.LOGICAL_DELETE_FIELD_NAME_DEFAULT;
        for (Field sourceField : source.fields()) {
            Field targetField = target.field(sourceField.name());
            if (!Objects.equals(sourceField.name(), delLogicalFieldName) && null == targetField) {
                missingFields.add(sourceField);
            }
        }
        return missingFields;
    }

    private SchemaBuilder createSchemaBuilderFromSource(final Schema source) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
            .name(source.name())
            .version(source.version());
        Map<String, String> parameters = source.parameters();
        if (null != parameters && !parameters.isEmpty()) {
            schemaBuilder.parameters(source.parameters());
        }
        return schemaBuilder;
    }

    private Schema newSchemaByAppendMissingFields(
        final SchemaBuilder schemaBuilder,
        final Schema target,
        final List<Field> toAppendFields) {
        toAppendFields.sort(Comparator.comparing((Field field) -> field.name().toUpperCase()));
        List<Field> newFields = new ArrayList<>(toAppendFields.size() + target.fields().size());
        newFields.addAll(target.fields());
        newFields.addAll(toAppendFields);
        for (Field field : newFields) {
            if (!SinkSchemas.containNameKey(field.schema())) {
                SinkSchema sinkSchema = SinkSchemas.sinkSchemaOf(field.schema());
                String sinkDataTypeName = sinkSchema.sinkDataTypeNameOf(field.schema());
                Schema schema = sinkSchema.schemaOf(sinkDataTypeName);
                schemaBuilder.field(field.name(), schema);
            } else {
                schemaBuilder.field(field.name(), field.schema());
            }
        }
        return schemaBuilder.build();
    }

    @Override
    public Schema getSchema(final Path path) {
        return readHiveTableSchema();
    }

    @Override
    public ProjectedResult projectRecord(final TopicPartition tp, final SinkRecord sinkRecord) {
        Schema sourceSchema = sinkRecord.valueSchema();
        Preconditions.checkNotNull(sourceSchema);

        // schema 不为 NUll，并且版本号匹配
        if (Objects.nonNull(currentSchema)) {
            String sourceKey = schemaUniqueKey(sourceSchema);
            String targetKey = schemaUniqueKey(currentSchema);
            if (Objects.equals(sourceKey, targetKey)) {
                log.info("Read meta schema from cache, check the schema not change, will keep using cache schema, "
                    + "source: {}, target: {}, tp: {}", sourceKey, targetKey, tp);

                return ProjectedResult.builder()
                    .projectedRecord(project(sinkRecord, currentSchema))
                    .needChangeSchema(false)
                    .currentSchema(currentSchema)
                    .build();
            }
            if (!Objects.equals(sourceKey, targetKey) && !isSupportSchemaChange) {
                log.info("Read meta schema from cache, check the schema changed,"
                    + "but configuration not supported schema change, will keep using cache schema, "
                    + "source:{}, target:{}, tp: {}", sourceKey, targetKey, tp);

                // 不支持 schema 变更，但是应该校验修改后的数据类型是否兼容
                schemaProjector.checkCompatibility(sourceSchema, currentSchema);
                return ProjectedResult.builder()
                    .projectedRecord(project(sinkRecord, currentSchema))
                    .needChangeSchema(false)
                    .currentSchema(currentSchema)
                    .build();
            }

            log.info("Read meta schema from cache, check the schema changed, "
                    + "configuration supported schema change, will read schema from meta, source: {}, target: {}, tp: {}",
                sourceKey, targetKey, tp
            );
        }

        // 读取 hive 元数据，获取当前 db 中最新的 schema 信息
        currentSchema = readHiveTableSchema(createSchemaBuilderFromSource(sourceSchema));
        schemaProjector.checkCompatibility(sourceSchema, currentSchema);

        // 配置不支持 schema 变更，直接使用 sink 端的 schema
        if (!isSupportSchemaChange) {
            log.info("Read meta schema from metastore, configuration not supported schema change, "
                + "will keep using hive meta schema, will cache schema tp: {}", tp);

            return ProjectedResult.builder()
                .projectedRecord(project(sinkRecord, currentSchema))
                .needChangeSchema(false)
                .currentSchema(currentSchema)
                .build();
        }

        // 检查是否新增加了字段，如果新增加了字段，执行 schema 变更的逻辑
        List<Field> missingFields = searchMissingFieldFromSource(sourceSchema, currentSchema);
        boolean shouldChangeSchema = !missingFields.isEmpty();

        // 如果没有增加字段忽略 schema 变更
        if (!shouldChangeSchema) {
            log.info("Read meta schema from metastore, check the schema not change, will cache schema tp: {}.", tp);

            return ProjectedResult.builder()
                .projectedRecord(project(sinkRecord, currentSchema))
                .needChangeSchema(false)
                .currentSchema(currentSchema)
                .build();
        }

        // 1. 增加了字段,需要 db 执行 schema 变更
        // 2. 失效缓存中的 schema 信息,目的是让缓存中的 schema 信息与 db 中保持一致，下次重新读取 hive 元数据最新 schema 信息
        SchemaBuilder schemaBuilder = createSchemaBuilderFromSource(sourceSchema);
        Schema newestSchema = newSchemaByAppendMissingFields(schemaBuilder, currentSchema, missingFields);
        ProjectedResult result = ProjectedResult.builder()
            .projectedRecord(project(sinkRecord, newestSchema))
            .needChangeSchema(true)
            .currentSchema(newestSchema)
            .build();
        currentSchema = NULL_SCHEMA;

        log.info("Read meta schema from metastore, check the schema changed, will lose efficacy cache's schema, tp: {}.", tp);

        return result;
    }

    @Override
    public HiveFactory getHiveFactory() {
        return new HiveMetaFactory();
    }
}
