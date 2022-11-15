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

import lombok.Getter;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

@Getter
public class SinkRecordField {

    private final Schema schema;

    private final String name;

    private final boolean isPrimaryKey;

    public SinkRecordField(final Schema schema, final String name, final boolean isPrimaryKey) {
        this.schema = schema;
        this.name = name;
        this.isPrimaryKey = isPrimaryKey;
    }

    /**
     * Get name of the schema.
     *
     * @return name of the schema
     */
    public String schemaName() {
        return schema.name();
    }

    /**
     * Get parameters of the schema.
     *
     * @return parameters of the schema
     */
    public Map<String, String> schemaParameters() {
        return schema.parameters();
    }

    /**
     * Get type of the schema.
     *
     * @return type of the schema
     */
    public Schema.Type schemaType() {
        return schema.type();
    }

    /**
     * Get this field is optional or not.
     *
     * @return true if this field is optional, otherwise false
     */
    public boolean isOptional() {
        return !isPrimaryKey && schema.isOptional();
    }

    /**
     * Get default value of this field.
     *
     * @return default value of this field
     */
    public Object defaultValue() {
        return schema.defaultValue();
    }

    @Override
    public String toString() {
        return "SinkRecordField{"
                + "schema=" + schema
                + ", name='" + name + '\''
                + ", isPrimaryKey=" + isPrimaryKey
                + '}';
    }
}
