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

package cn.xdf.acdc.connect.hdfs;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.common.StoreConstants;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.storage.StorageSinkConnectorConfig;
import com.google.common.base.Strings;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class StorageSinkTestBase {

    // CHECKSTYLE:OFF

    protected static final String DEFAULT_URL = "hdfs://localhost:9089";


    protected static final String TOPIC = "test-topic";

    protected static final int PARTITION = 12;

    protected static final int PARTITION2 = 13;

    protected static final int PARTITION3 = 14;

    protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);

    protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);

    protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);

    protected static final long TIMESTAMP = 12L;

    protected Map<String, String> properties;

    protected String url;

    protected MockSinkTaskContext context;

    protected Map<String, String> createProps() {
        Map<String, String> props = new HashMap<>();
        props.put(StorageCommonConfig.STORE_URL_CONFIG, Strings.isNullOrEmpty(url) ? DEFAULT_URL : url);
        props.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "3");
        props.put(SinkConfig.DESTINATIONS, StoreConstants.HIVE_DB + "." + StoreConstants.HIVE_TABLE);
        props.put(SinkConfig.DESTINATIONS + "." + StoreConstants.HIVE_DB + "." + StoreConstants.HIVE_TABLE + ".fields.whitelist", createFiledWhitelist());
        props.put(HdfsSinkConfig.STORAGE_ROOT_PATH, StoreConstants.HDFS_ROOT);
        return props;
    }

    private String createFiledWhitelist() {
        Schema schema = createPromotableSchema();
        StringBuilder builder = new StringBuilder();
        schema.fields().forEach(s -> builder.append(s.name()).append(","));
        return builder.toString().replaceAll(",$", "");
    }

    protected String generateEncodedPartitionFromMap(final Map<String, Object> fieldMapping) {
        String delim = StorageCommonConfig.DIRECTORY_DELIM_DEFAULT;
        return Utils.mkString(fieldMapping, "", "", "=", delim);
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct().name("record").version(1)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .build();
    }

    protected Schema createPromotableSchema() {
        return SchemaBuilder.struct().name("record").version(1)
            .field("boolean", Schema.STRING_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double1", Schema.FLOAT64_SCHEMA)
            .field("double2", Schema.FLOAT64_SCHEMA)
            .field("double3", Schema.FLOAT64_SCHEMA)
            .field("double4", Schema.FLOAT64_SCHEMA)
            .field("double5", Schema.FLOAT64_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
    }

    protected SinkRecord createPromotableSchemaSinkRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(1)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double1", Schema.FLOAT64_SCHEMA)
            .field("double2", Schema.FLOAT64_SCHEMA)
            .field("double3", Schema.FLOAT64_SCHEMA)
            .field("double4", Schema.FLOAT64_SCHEMA)
            .field("double5", Schema.FLOAT64_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double1", 12.2)
            .put("double2", 12.2)
            .put("double3", 12.2)
            .put("double4", 12.2)
            .put("double5", 12.2)
            .put("double6", 12.2);
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected Struct createStruct() {
        return new Struct(createSchema())
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double", 12.2);
    }

    protected SinkRecord createSinkRecord() {
        Struct record = createRecord(createSchema());
        Schema schema = createSchema();
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected SinkRecord createSinkRecord(long timestamp) {
        Schema schema = createSchemaWithTimestampField();
        Struct record = createRecordWithTimestampField(schema, timestamp);
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L,
            timestamp, TimestampType.CREATE_TIME);
    }

    protected Struct createRecord(final Schema schema) {
        return new Struct(schema)
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double", 12.2);
    }

    protected SinkRecord createTypeChangeWithNotCompatibilityRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
            .field("boolean", Schema.INT32_SCHEMA)
            .field("int", Schema.BOOLEAN_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double1", Schema.FLOAT64_SCHEMA)
            .field("double2", Schema.FLOAT64_SCHEMA)
            .field("double3", Schema.FLOAT64_SCHEMA)
            .field("double4", Schema.FLOAT64_SCHEMA)
            .field("double5", Schema.FLOAT64_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("boolean", 100)
            .put("int", true)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double1", 12.2)
            .put("double2", 12.2)
            .put("double3", 12.2)
            .put("double4", 12.2)
            .put("double5", 12.2)
            .put("double6", 12.2);

        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected SinkRecord createTypeChangeRecordWithStringCompatibility() {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
            .field("boolean", Schema.INT32_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double1", Schema.FLOAT64_SCHEMA)
            .field("double2", Schema.FLOAT64_SCHEMA)
            .field("double3", Schema.FLOAT64_SCHEMA)
            .field("double4", Schema.FLOAT64_SCHEMA)
            .field("double5", Schema.FLOAT64_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("boolean", 100)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double1", 12.2)
            .put("double2", 12.2)
            .put("double3", 12.2)
            .put("double4", 12.2)
            .put("double5", 12.2)
            .put("double6", 12.2);

        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected SinkRecord createNotPromotableRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT64_SCHEMA)
            .field("long", Schema.FLOAT32_SCHEMA)
            .field("float", Schema.FLOAT64_SCHEMA)
            .field("double1", Schema.INT8_SCHEMA)
            .field("double2", Schema.FLOAT64_SCHEMA)
            .field("double3", Schema.FLOAT64_SCHEMA)
            .field("double4", Schema.FLOAT64_SCHEMA)
            .field("double5", Schema.FLOAT64_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("boolean", true)
            .put("int", 12L)
            .put("long", 12.2f)
            .put("float", 12.2)
            .put("double1", new Byte("12"))
            .put("double2", 12.2)
            .put("double3", 12.2)
            .put("double4", 12.2)
            .put("double5", 12.2)
            .put("double6", 12.2);

        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected SinkRecord createPromotableSinkRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double1", Schema.INT8_SCHEMA)
            .field("double2", Schema.INT16_SCHEMA)
            .field("double3", Schema.INT32_SCHEMA)
            .field("double4", Schema.INT64_SCHEMA)
            .field("double5", Schema.FLOAT32_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double1", new Byte("12"))
            .put("double2", new Short("12"))
            .put("double3", 12)
            .put("double4", 12L)
            .put("double5", 12.2f)
            .put("double6", 12.2);

        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected SinkRecord createDecreaseFieldSinkRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("double6", 12.2);
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected SinkRecord createIncreaseFieldSinkRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double1", Schema.FLOAT64_SCHEMA)
            .field("double2", Schema.FLOAT64_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("double3", Schema.FLOAT64_SCHEMA)
            .field("double4", Schema.FLOAT64_SCHEMA)
            .field("double5", Schema.FLOAT64_SCHEMA)
            .field("double6", Schema.FLOAT64_SCHEMA)
            .build();
        Struct struct = new Struct(schema)
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double1", 12.2)
            .put("double2", 12.2)
            .put("string", "test")
            .put("double3", 12.2)
            .put("double4", 12.2)
            .put("double5", 12.2)
            .put("double6", 12.2);
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L,
            0L, TimestampType.CREATE_TIME);
    }

    protected Schema createNewSchema() {
        return SchemaBuilder.struct().name("record").version(2)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("string", SchemaBuilder.string().defaultValue("abc").build())
            .build();
    }

    protected Struct createNewRecord(final Schema newSchema) {
        return new Struct(newSchema)
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double", 12.2)
            .put("string", "def");
    }

    protected Schema createSchemaNoVersion() {
        return SchemaBuilder.struct().name("record")
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .build();
    }

    protected Schema createSchemaWithTimestampField() {
        return createSchemaWithTimestampField(Schema.INT64_SCHEMA);
    }

    protected Schema createSchemaWithTimestampField(final Schema timestampSchema) {
        return SchemaBuilder.struct().name("record").version(1)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("string", SchemaBuilder.string().defaultValue("abc").build())
            .field("timestamp", timestampSchema)
            .build();
    }

    protected Struct createRecordWithTimestampField(final Schema newSchema, long timestamp) {
        return createRecordWithTimestampField(newSchema, (Object) timestamp);
    }

    protected Struct createRecordWithTimestampField(final Schema newSchema, final Object timestamp) {
        return new Struct(newSchema)
            .put("boolean", true)
            .put("int", 12)
            .put("long", 12L)
            .put("float", 12.2f)
            .put("double", 12.2)
            .put("string", "def")
            .put("timestamp", timestamp);
    }

    protected Struct createRecordWithNestedTimestampField(long timestamp) {
        Schema nestedChildSchema = createSchemaWithTimestampField();
        Schema nestedSchema = SchemaBuilder.struct().field("nested", nestedChildSchema);
        return new Struct(nestedSchema)
            .put("nested", createRecordWithTimestampField(nestedChildSchema, timestamp));
    }

    protected Map<String, Object> createMapWithTimestampField(long timestamp) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("boolean", true);
        m.put("int", 12);
        m.put("long", 12L);
        m.put("float", 12.2f);
        m.put("double", 12.2);
        m.put("string", "def");
        m.put("timestamp", timestamp);
        return m;
    }

    protected SinkRecord createSinkRecordWithNestedTimestampField(long timestamp) {
        Struct record = createRecordWithNestedTimestampField(timestamp);
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, record.schema(), record, 0L,
            timestamp, TimestampType.CREATE_TIME);
    }

    public void setUp() throws Exception {
        properties = createProps();
        Set<TopicPartition> assignment = new HashSet<>();
        assignment.add(TOPIC_PARTITION);
        assignment.add(TOPIC_PARTITION2);
        context = new MockSinkTaskContext(assignment);
    }

    @After
    public void tearDown() throws Exception {
    }

    protected static class MockSinkTaskContext implements SinkTaskContext {

        private final Map<TopicPartition, Long> offsets;

        private long timeoutMs;

        private Set<TopicPartition> assignment;

        public MockSinkTaskContext(final Set<TopicPartition> assignment) {
            this.offsets = new HashMap<>();
            this.timeoutMs = -1L;
            this.assignment = assignment;
        }

        @Override
        public Map<String, String> configs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void offset(final Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        @Override
        public void offset(final TopicPartition tp, long offset) {
            offsets.put(tp, offset);
        }

        /**
         * Get offsets that SinkTask intends to reset. Used by Connect framework.
         * @return the map of offsets
         */
        public Map<TopicPartition, Long> offsets() {
            return offsets;
        }

        @Override
        public void timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by Connect framework.
         * @return the backoff timeout in milliseconds.
         */
        public long timeout() {
            return timeoutMs;
        }

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        public void setAssignment(final Set<TopicPartition> nextAssignment) {
            assignment = nextAssignment;
        }

        @Override
        public void pause(final TopicPartition... partitions) {
        }

        @Override
        public void resume(final TopicPartition... partitions) {
        }

        @Override
        public void requestCommit() {
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            throw new UnsupportedOperationException("ErrantRecordReporter is undefined for this class");
        }
    }

}
