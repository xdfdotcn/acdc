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

package cn.xdf.acdc.connect.hdfs.schema;

import cn.xdf.acdc.connect.hdfs.common.Schemas;
import cn.xdf.acdc.connect.hdfs.hive.HiveMetaStore;
import cn.xdf.acdc.connect.hdfs.hive.HiveTable;
import cn.xdf.acdc.connect.hdfs.hive.HiveTestBase;
import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.Partitioner;
import cn.xdf.acdc.connect.hdfs.writer.StoreContext;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 {@link cn.xdf.acdc.connect.hdfs.schema.SinkSchemas}.
 */
public class SinkSchemasTest extends HiveTestBase {

    private Partitioner partitioner;

    private StoreConfig storeConfig;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        HiveMetaStore hiveMetaStore = new HiveMetaStore(connectorConfig);
        StoreContext storeContext = StoreContext.buildContext(connectorConfig, hiveMetaStore);
        partitioner = storeContext.getPartitioner();
        storeConfig = storeContext.getStoreConfig();
    }

    @Test
    public void testConvertFieldWithAllHiveTableFieldType() throws HiveException, SerDeException {
        List<FieldSchema> fieldSchemaList = Schemas.createHivePrimitiveSchemaWithAllFieldType();
        Table table = new HiveTable().createTable(url, fieldSchemaList, partitioner, storeConfig.textSeparator());
        hiveMetaStore.createTable(table);

        // Convert hive primitive schema to connect schema.
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
            .name("record")
            .version(1);
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
        Schema schema = SinkSchemas.convertToStructSchema(structFieldList, schemaBuilder);
        Assert.assertTrue(schema.fields().size() == fieldSchemaList.size());
        Assert.assertEquals(Type.INT8, schema.field("tinyint_schema").schema().type());
        Assert.assertEquals(Type.INT16, schema.field("smallint_schema").schema().type());
        Assert.assertEquals(Type.INT32, schema.field("int_schema").schema().type());
        Assert.assertEquals(Type.INT64, schema.field("bigint_schema").schema().type());
        Assert.assertEquals(Type.FLOAT32, schema.field("float_schema").schema().type());
        Assert.assertEquals(Type.FLOAT64, schema.field("double_schema").schema().type());
        Assert.assertEquals(Type.BOOLEAN, schema.field("boolean_schema").schema().type());
        Assert.assertEquals(Type.STRING, schema.field("string_schema").schema().type());
        Assert.assertEquals(Type.STRING, schema.field("varchar_schema").schema().type());
        Assert.assertEquals(Type.STRING, schema.field("char_schema").schema().type());
        Assert.assertEquals(Type.BYTES, schema.field("binary_schema").schema().type());
        Assert.assertEquals(Decimal.LOGICAL_NAME, schema.field("decimal_schema").schema().name());
        Assert.assertEquals(Date.LOGICAL_NAME, schema.field("date_schema").schema().name());
        Assert.assertEquals(Timestamp.LOGICAL_NAME, schema.field("timestamp_schema").schema().name());
    }
}
