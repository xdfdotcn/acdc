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

import java.util.Objects;

@Getter
public class SchemaPair {

    private Schema keySchema;

    private Schema valueSchema;

    public SchemaPair(final Schema keySchema, final Schema valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaPair that = (SchemaPair) o;
        return Objects.equals(keySchema, that.keySchema)
                && Objects.equals(valueSchema, that.valueSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySchema, valueSchema);
    }

    @Override
    public String toString() {
        return String.format("<SchemaPair: %s, %s>", keySchema, valueSchema);
    }
}
