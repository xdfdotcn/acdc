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

import cn.xdf.acdc.connect.hdfs.schema.SinkSchemas;
import com.google.common.base.Preconditions;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SchemaProjector {

    public SchemaProjector() {
    }

    /**
     * Project sink record.
     * @param source source schema
     * @param record sink record
     * @param target  target schema
     * @return projected sink record
     * @throws SchemaProjectorException exception on project record
     */
    public Object project(final Schema source,
        final Object record,
        final Schema target) throws SchemaProjectorException {
        if (source.isOptional() && !target.isOptional()) {
            if (target.defaultValue() != null) {
                return record != null ? projectRequiredSchema(source, record, target) : target.defaultValue();
            } else {
                throw new SchemaProjectorException("Writer schema is optional, however, target schema does not provide a default value.");
            }
        } else {
            return record != null ? projectRequiredSchema(source, record, target) : null;
        }
    }

    private Object projectRequiredSchema(
        final Schema source,
        final Object record,
        final Schema target) throws SchemaProjectorException {
        if (target.type() == Type.STRUCT) {
            return projectStruct(source, (Struct) record, target);
        }
        return SinkSchemas.sinkSchemaOf(target).convertToJavaTypeValue(source, record);
    }

    private Object projectStruct(
        final Schema source,
        final Struct sourceStruct,
        final Schema target) throws SchemaProjectorException {
        Struct targetStruct = new Struct(target);
        Iterator fieldIterator = target.fields().iterator();
        while (fieldIterator.hasNext()) {
            Field targetField = (Field) fieldIterator.next();
            String fieldName = targetField.name();
            Field sourceField = source.field(fieldName);
            if (sourceField != null) {
                Object sourceFieldValue = sourceStruct.get(fieldName);

                try {
                    Object targetFieldValue = project(sourceField.schema(), sourceFieldValue, targetField.schema());
                    targetStruct.put(fieldName, targetFieldValue);
                } catch (SchemaProjectorException e) {
                    throw new SchemaProjectorException("Error projecting " + sourceField.name(), e);
                }
            } else if (!targetField.schema().isOptional()) {
                if (targetField.schema().defaultValue() == null) {
                    throw new SchemaProjectorException("Required field `" + fieldName + "` is missing from source schema: " + source);
                }

                targetStruct.put(fieldName, targetField.schema().defaultValue());
            }
        }
        return targetStruct;
    }

    /**
     Project sink record.
     * @param record  sink record
     * @param currentKeySchema current key schema
     * @param currentValueSchema current value schema
     * @return projected sink record
     * @throws SchemaProjectorException exception on project record
     */
    public SinkRecord projectRecord(
        final SinkRecord record,
        final Schema currentKeySchema,
        final Schema currentValueSchema
    ) {
        Map.Entry<Object, Object> projected = projectInternal(
            record,
            currentKeySchema,
            currentValueSchema
        );

        // Just reference comparison here.
        return projected.getKey() == record.key() && projected.getValue() == record.value()
            ? record
            : new SinkRecord(
                record.topic(),
                record.kafkaPartition(),
                currentKeySchema,
                projected.getKey(),
                currentValueSchema,
                projected.getValue(),
                record.kafkaOffset(),
                record.timestamp(),
                record.timestampType()
            );
    }

    private Map.Entry<Object, Object> projectInternal(
        final ConnectRecord<?> record,
        final Schema currentKeySchema,
        final Schema currentValueSchema
    ) {
        Preconditions.checkArgument(currentValueSchema.type() == Type.STRUCT);
        Preconditions.checkArgument(record.valueSchema().type() == Type.STRUCT);

        // Currently in Storage only value schemas are considered for compatibility resolution.
        Object value = projectInternal(record.valueSchema(), record.value(), currentValueSchema);
        return new AbstractMap.SimpleEntry<>(record.key(), value);
    }

    private Object projectInternal(
        final Schema originalSchema,
        final Object value,
        final Schema currentSchema
    ) {
        if (Objects.equals(originalSchema, currentSchema)) {
            return value;
        }
        return project(originalSchema, value, currentSchema);
    }


    /**
     * Data type can be compatibility.
     * @param source source filed schema
     * @param target  target filed schema
     * @return boolean
     */
    protected boolean isCompatibility(
        final Schema source,
        final Schema target) {
        return SinkSchemas.sinkSchemaOf(target).isCompatibility(source);
    }

    /**
     * Data type maybe compatible.
     * @param source source filed schema
     * @param target  target filed schema
     */
    public void checkCompatibility(final Schema source, final Schema target) {
        if (Type.STRUCT != source.type() || Type.STRUCT != target.type()) {
            throw new ConnectException(String.format(
                "Schema type must be of type STRUCT, but was:%s, %s",
                source,
                target)
            );
        }
        for (Field targetFiled : target.fields()) {
            Field sourceField = source.field(targetFiled.name());
            if (Objects.isNull(sourceField)) {
                continue;
            }
            Schema sourceFiledSchema = sourceField.schema();
            Schema targetFiledSchema = targetFiled.schema();
            boolean isCompatibility = isCompatibility(sourceFiledSchema, targetFiledSchema);
            if (!isCompatibility) {
                throw new ConnectException(String.format(
                    "Schema data type are not compatible, "
                        + "source schema type: %s, "
                        + "target schema type: %s, "
                        + "source schema name: %s, "
                        + "target schema name: %s, "
                        + "source field name: %s, "
                        + "target field name: %s, "
                        + "target field parameters: %s",
                    sourceFiledSchema,
                    targetFiledSchema,
                    sourceFiledSchema.name(),
                    targetFiledSchema.name(),
                    sourceField.name(),
                    targetFiled.name(),
                    targetFiledSchema.parameters()
                )
                );
            }
        }
    }
}
