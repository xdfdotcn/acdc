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

package cn.xdf.acdc.connect.core.sink.metadata;

import cn.xdf.acdc.connect.core.util.config.PrimaryKeyMode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldsMetadataTest {

    private static final Schema SIMPLE_PRIMITIVE_SCHEMA = Schema.INT64_SCHEMA;

    private static final Schema SIMPLE_STRUCT_SCHEMA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

    private static final Schema SIMPLE_MAP_SCHEMA = SchemaBuilder.map(SchemaBuilder.INT64_SCHEMA, Schema.STRING_SCHEMA);

    private static FieldsMetadata extract(final PrimaryKeyMode pkMode, final List<String> pkFields, final Schema keySchema, final Schema valueSchema) {
        return extract(pkMode, pkFields, Collections.<String>emptySet(), keySchema, valueSchema);
    }

    private static FieldsMetadata extract(final PrimaryKeyMode pkMode, final List<String> pkFields, final Set<String> whitelist, final Schema keySchema, final Schema valueSchema) {
        return FieldsMetadata.extract("table", pkMode, pkFields, whitelist, keySchema, valueSchema);
    }

    @Test(expected = ConnectException.class)
    public void valueSchemaMustBePresentForPkModeRecordValue() {
        extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.<String>emptyList(),
                SIMPLE_PRIMITIVE_SCHEMA,
                null
        );
    }

    @Test(expected = ConnectException.class)
    public void valueSchemaMustBeStructIfPresent() {
        extract(
                PrimaryKeyMode.KAFKA,
                Collections.<String>emptyList(),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_PRIMITIVE_SCHEMA
        );
    }

    @Test
    public void missingValueSchemaCanBeOk() {
        Assert.assertEquals(
                new HashSet<>(Collections.singletonList("name")),
                extract(
                        PrimaryKeyMode.RECORD_KEY,
                        Collections.<String>emptyList(),
                        SIMPLE_STRUCT_SCHEMA,
                        null
                ).getAllFields().keySet()
        );

        // this one is a bit weird, only columns being inserted would be kafka coords... but not sure should explicitly disallow!
        Assert.assertEquals(
                new HashSet<>(Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset")),
                extract(
                        PrimaryKeyMode.KAFKA,
                        Collections.<String>emptyList(),
                        null,
                        null
                ).getAllFields().keySet()
        );
    }

    @Test(expected = ConnectException.class)
    public void metadataMayNotBeEmpty() {
        extract(
                PrimaryKeyMode.NONE,
                Collections.<String>emptyList(),
                null,
                null
        );
    }

    @Test
    public void kafkaPkMode() {
        FieldsMetadata metadata = extract(
                PrimaryKeyMode.KAFKA,
                Collections.<String>emptyList(),
                null,
                SIMPLE_STRUCT_SCHEMA
        );
        Assert.assertEquals(new HashSet<>(Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset")), metadata.getKeyFieldNames());
        Assert.assertEquals(Collections.singleton("name"), metadata.getNonKeyFieldNames());

        SinkRecordField topicField = metadata.getAllFields().get("__connect_topic");
        Assert.assertEquals(Schema.Type.STRING, topicField.schemaType());
        Assert.assertTrue(topicField.isPrimaryKey());
        Assert.assertFalse(topicField.isOptional());

        SinkRecordField partitionField = metadata.getAllFields().get("__connect_partition");
        Assert.assertEquals(Schema.Type.INT32, partitionField.schemaType());
        Assert.assertTrue(partitionField.isPrimaryKey());
        Assert.assertFalse(partitionField.isOptional());

        SinkRecordField offsetField = metadata.getAllFields().get("__connect_offset");
        Assert.assertEquals(Schema.Type.INT64, offsetField.schemaType());
        Assert.assertTrue(offsetField.isPrimaryKey());
        Assert.assertFalse(offsetField.isOptional());
    }

    @Test
    public void kafkaPkModeCustomNames() {
        List<String> customKeyNames = Arrays.asList("the_topic", "the_partition", "the_offset");
        FieldsMetadata metadata = extract(
                PrimaryKeyMode.KAFKA,
                customKeyNames,
                null,
                SIMPLE_STRUCT_SCHEMA
        );
        Assert.assertEquals(new HashSet<>(customKeyNames), metadata.getKeyFieldNames());
        Assert.assertEquals(Collections.singleton("name"), metadata.getNonKeyFieldNames());
    }

    @Test(expected = ConnectException.class)
    public void kafkaPkModeBadFieldSpec() {
        extract(
                PrimaryKeyMode.KAFKA,
                Collections.singletonList("lone"),
                null,
                SIMPLE_STRUCT_SCHEMA
        );
    }

    /**
     * RECORD_KEY test cases: if keySchema is a struct, pkCols must be a subset of the keySchema fields.
     */
    @Test
    public void recordKeyPkModePrimitiveKey() {
        FieldsMetadata metadata = extract(
                PrimaryKeyMode.RECORD_KEY,
                Collections.singletonList("the_pk"),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
        );

        Assert.assertEquals(Collections.singleton("the_pk"), metadata.getKeyFieldNames());

        Assert.assertEquals(Collections.singleton("name"), metadata.getNonKeyFieldNames());

        Assert.assertEquals(SIMPLE_PRIMITIVE_SCHEMA.type(), metadata.getAllFields().get("the_pk").schemaType());
        Assert.assertTrue(metadata.getAllFields().get("the_pk").isPrimaryKey());
        Assert.assertFalse(metadata.getAllFields().get("the_pk").isOptional());

        Assert.assertEquals(Schema.Type.STRING, metadata.getAllFields().get("name").schemaType());
        Assert.assertFalse(metadata.getAllFields().get("name").isPrimaryKey());
        Assert.assertFalse(metadata.getAllFields().get("name").isOptional());
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeWithPrimitiveKeyButMultiplePkFieldsSpecified() {
        extract(
                PrimaryKeyMode.RECORD_KEY,
                Arrays.asList("pk1", "pk2"),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeButKeySchemaMissing() {
        extract(
                PrimaryKeyMode.RECORD_KEY,
                Collections.<String>emptyList(),
                null,
                SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeButKeySchemaAsNonStructCompositeType() {
        extract(
                PrimaryKeyMode.RECORD_KEY,
                Collections.<String>emptyList(),
                SIMPLE_MAP_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeWithStructKeyButMissingField() {
        extract(
                PrimaryKeyMode.RECORD_KEY,
                Collections.singletonList("nonexistent"),
                SIMPLE_STRUCT_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordValuePkModeWithMissingPkField() {
        extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("nonexistent"),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test
    public void recordValuePkModeWithValidPkFields() {
        final FieldsMetadata metadata = extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("name"),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
        );

        Assert.assertEquals(Collections.singleton("name"), metadata.getKeyFieldNames());
        Assert.assertEquals(Collections.emptySet(), metadata.getNonKeyFieldNames());

        Assert.assertEquals(Schema.Type.STRING, metadata.getAllFields().get("name").schemaType());
        Assert.assertTrue(metadata.getAllFields().get("name").isPrimaryKey());
        Assert.assertFalse(metadata.getAllFields().get("name").isOptional());
    }

    @Test
    public void recordValuePkModeWithPkFieldsAndWhitelistFiltering() {
        final Schema valueSchema =
                SchemaBuilder.struct()
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .field("field4", Schema.INT64_SCHEMA)
                        .build();

        final FieldsMetadata metadata = extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field1"),
                new HashSet<>(Arrays.asList("field2", "field4")),
                null,
                valueSchema
        );

        Assert.assertEquals(Collections.singleton("field1"), metadata.getKeyFieldNames());
        Assert.assertEquals(new HashSet<>(Arrays.asList("field2", "field4")), metadata.getNonKeyFieldNames());
    }

    @Test
    public void recordValuePkModeWithFieldsRetainOriginalOrdering() {
        final Schema valueSchema =
                SchemaBuilder.struct()
                        .field("field4", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .build();

        FieldsMetadata metadata = extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field4"),
                new HashSet<>(Arrays.asList("field3", "field1", "field2")),
                null,
                valueSchema
        );

        Assert.assertEquals(Arrays.asList("field4", "field2", "field1", "field3"),
                new ArrayList<>(metadata.getAllFields().keySet()));

        metadata = extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field1"),
                new HashSet<>(Arrays.asList("field4", "field3")),
                null,
                valueSchema
        );

        Assert.assertEquals(Arrays.asList("field4", "field1", "field3"), new ArrayList<>(metadata.getAllFields().keySet()));

        final Schema keySchema =
                SchemaBuilder.struct()
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .build();

        metadata = extract(
                PrimaryKeyMode.RECORD_KEY,
                Arrays.asList("field2", "field3", "field1"),
                new HashSet<>(Arrays.asList("field3", "field1")),
                keySchema,
                null
        );

        Assert.assertEquals(Arrays.asList("field1", "field2", "field3"), new ArrayList<>(metadata.getAllFields().keySet()));
    }

    @Test
    public void testExtractWithMetaFieldsShouldBeRemoved() {
        final Schema valueSchema =
                SchemaBuilder.struct()
                        .field("field4", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .field("__table", Schema.STRING_SCHEMA)
                        .field("__delete", Schema.STRING_SCHEMA)
                        .field("__op", Schema.STRING_SCHEMA)
                        .build();

        FieldsMetadata metadata = extract(
                PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field4"),
                Sets.newHashSet(),
                null,
                valueSchema
        );

        Assert.assertEquals(Arrays.asList("field4", "field2", "field1", "field3"),
                new ArrayList<>(metadata.getAllFields().keySet()));

    }
}
