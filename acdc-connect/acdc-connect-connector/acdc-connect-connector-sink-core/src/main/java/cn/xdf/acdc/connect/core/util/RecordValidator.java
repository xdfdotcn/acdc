/*
 * Copyright 2020 Confluent Inc.
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

package cn.xdf.acdc.connect.core.util;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

@FunctionalInterface
public interface RecordValidator {

    RecordValidator NO_OP = record -> {
    };

    /**
     * Create a record validator by sink config.
     *
     * @param config sink config
     * @return record validator
     */
    static RecordValidator create(final SinkConfig config) {
        RecordValidator requiresKey = requiresKey(config);
        RecordValidator requiresValue = requiresValue(config);

        RecordValidator keyValidator = NO_OP;
        RecordValidator valueValidator = NO_OP;
        switch (config.getPkMode()) {
            case RECORD_KEY:
                keyValidator = keyValidator.and(requiresKey);
                break;
            case RECORD_VALUE:
            case NONE:
                valueValidator = valueValidator.and(requiresValue);
                break;
            case KAFKA:
            default:
                // no primary key is required
                break;
        }

        if (config.isDeleteEnabled()) {
            // When delete is enabled, we need a key
            keyValidator = keyValidator.and(requiresKey);
        } else {
            // When delete is disabled, we need non-tombstone values
            valueValidator = valueValidator.and(requiresValue);
        }

        // Compose the validator that may or may be NO_OP
        return keyValidator.and(valueValidator);
    }

    /**
     * Get a record validator for requires value.
     *
     * @param config sink config
     * @return validator for requires value
     */
    static RecordValidator requiresValue(final SinkConfig config) {
        return record -> {
            Schema valueSchema = record.valueSchema();
            if (record.value() != null
                    && valueSchema != null
                    && valueSchema.type() == Schema.Type.STRUCT) {
                return;
            }
            throw new ConnectException(
                    String.format(
                            "Sink connector '%s' is configured with '%s=%s' and '%s=%s' and therefore requires "
                                    + "records with a non-null Struct value and non-null Struct schema, "
                                    + "but found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) "
                                    + "with a %s value and %s value schema.",
                            config.getConnectorName(),
                            SinkConfig.DELETE_ENABLED,
                            config.isDeleteEnabled(),
                            SinkConfig.PK_MODE,
                            config.getPkMode().toString().toLowerCase(),
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset(),
                            record.timestamp(),
                            StringUtils.valueTypeOrNull(record.value()),
                            StringUtils.schemaTypeOrNull(record.valueSchema())
                    )
            );
        };
    }

    /**
     * Get a record validator for requires key.
     *
     * @param config sink config
     * @return validator for requires key
     */
    static RecordValidator requiresKey(final SinkConfig config) {
        return record -> {
            Schema keySchema = record.keySchema();
            if (record.key() != null
                    && keySchema != null
                    && (keySchema.type() == Schema.Type.STRUCT || keySchema.type().isPrimitive())) {
                return;
            }
            throw new ConnectException(
                    String.format(
                            "Sink connector '%s' is configured with '%s=%s' and '%s=%s' and therefore requires "
                                    + "records with a non-null key and non-null Struct or primitive key schema, "
                                    + "but found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) "
                                    + "with a %s key and %s key schema.",
                            config.getConnectorName(),
                            SinkConfig.DELETE_ENABLED,
                            config.isDeleteEnabled(),
                            SinkConfig.PK_MODE,
                            config.getPkMode().toString().toLowerCase(),
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset(),
                            record.timestamp(),
                            StringUtils.valueTypeOrNull(record.key()),
                            StringUtils.schemaTypeOrNull(record.keySchema())
                    )
            );
        };
    }

    /**
     * Do validate for the sink record.
     *
     * @param record record to be validated
     */
    void validate(SinkRecord record);

    /**
     * Add a record validator to the chain.
     *
     * @param other record validator to be added
     * @return new validator include new and old validator
     */
    default RecordValidator and(final RecordValidator other) {
        if (other == null || other == NO_OP || other == this) {
            return this;
        }
        if (this == NO_OP) {
            return other;
        }
        RecordValidator thisValidator = this;
        return record -> {
            thisValidator.validate(record);
            other.validate(record);
        };
    }

}
