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
import cn.xdf.acdc.connect.hdfs.format.AbstractSchemaFileReader;
import cn.xdf.acdc.connect.hdfs.hive.HiveFactory;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.storage.HdfsFileOperator;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

@Slf4j
public class OrcFileReader extends AbstractSchemaFileReader {

    public OrcFileReader(
        final HdfsSinkConfig sinkConfig,
        final StoreConfig storeConfig,
        final HdfsFileOperator storage
    ) {
        super(sinkConfig, storeConfig, storage);
    }

    @Override
    public Schema getSchema(final Path path) {
        try {
            log.debug("Opening ORC record reader for: {}", path);

            if (null == path) {
                return null;
            }
            ReaderOptions readerOptions = new ReaderOptions(getHdfsSinkConfig().getHadoopConfiguration());
            Reader reader = OrcFile.createReader(path, readerOptions);

            if (reader.getObjectInspector().getCategory() == ObjectInspector.Category.STRUCT) {
                SchemaBuilder schemaBuilder = SchemaBuilder.struct().name("record").version(1);
                StructObjectInspector objectInspector = (StructObjectInspector) reader.getObjectInspector();

                for (StructField schema : objectInspector.getAllStructFieldRefs()) {
                    ObjectInspector fieldObjectInspector = schema.getFieldObjectInspector();
                    String typeName = fieldObjectInspector.getTypeName();
                    Schema.Type schemaType;

                    switch (fieldObjectInspector.getCategory()) {
                        case PRIMITIVE:
                            PrimitiveTypeEntry typeEntry = PrimitiveObjectInspectorUtils
                                .getTypeEntryFromTypeName(typeName);
                            if (java.sql.Date.class.isAssignableFrom(typeEntry.primitiveJavaClass)) {
                                schemaType = Date.SCHEMA.type();
                            } else if (java.sql.Timestamp.class.isAssignableFrom(typeEntry.primitiveJavaClass)) {
                                schemaType = Timestamp.SCHEMA.type();
                            } else {
                                schemaType = ConnectSchema.schemaType(typeEntry.primitiveJavaClass);
                            }
                            break;
                        case LIST:
                            schemaType = Schema.Type.ARRAY;
                            break;
                        case MAP:
                            schemaType = Schema.Type.MAP;
                            break;
                        default:
                            throw new DataException("Unknown type " + fieldObjectInspector.getCategory().name());
                    }

                    schemaBuilder.field(schema.getFieldName(), SchemaBuilder.type(schemaType).build());
                }

                return schemaBuilder.build();
            } else {
                throw new ConnectException(
                    "Top level type must be of type STRUCT, but was "
                        + reader.getObjectInspector().getCategory().name()
                );
            }
        } catch (IOException e) {
            throw new ConnectException("Failed to get schema for file " + path, e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public HiveFactory getHiveFactory() {
        return new OrcHiveFactory();
    }
}
