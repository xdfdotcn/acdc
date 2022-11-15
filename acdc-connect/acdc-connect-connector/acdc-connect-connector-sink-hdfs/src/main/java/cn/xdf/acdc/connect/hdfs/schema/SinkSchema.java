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

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;

public interface SinkSchema {

    /**
     * Schema name.
     * @return the schema's name
     */
    String name();

    /**
     * Create a new schema by sink data type name.
     * @param sinkDataTypeName  sink db's data type
     * @return type name
     */
    Schema schemaOf(String sinkDataTypeName);

    /**
     * Whether or not promotable.
     * @param schema  schema schema type
     * @return false is representative not promotable
     */
    boolean isPromotable(Type schema);

    /**
     * Whether or not compatibility.
     * @param source source schema
     * @return false is representative not compatibility
     */
    boolean isCompatibility(Schema source);

    /**
     * Convert to a Java type.
     * @param source  source schema
     * @param recordValue  source record value
     * @return java type value
     */
    Object convertToJavaTypeValue(Schema source, Object recordValue);

    /**
     * Convert to a DB type.
     * @param source  source schema
     * @param recordValue  source record value
     * @return db type value
     */
    Object convertToDbTypeValue(Schema source, Object recordValue);

    /**
     * Convert to a DB type.
     * @param schema  specified schema
     * @return db type name
     */
    String sinkDataTypeNameOf(Schema schema);

    /**
     * The default method,convert the bytes to a String.
     * @param schema  specified schema
     * @param recordValue  record value
     * @return string
     */
    default Object convertBytesToString(Schema schema, Object recordValue) {
        try {
            byte[] bytes = convertBytesToBytes(schema, recordValue);
            return new String(bytes, HdfsSinkConstants.UTF8_CHARACTER);
        } catch (UnsupportedEncodingException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * The default method,convert the bytes to a bytes.
     * @param schema  specified schema
     * @param recordValue  record value
     * @return string
     */
    default byte[] convertBytesToBytes(Schema schema, Object recordValue) {
        final byte[] bytes;
        if (recordValue instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) recordValue).slice();
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        } else {
            bytes = (byte[]) recordValue;
        }
        return bytes;
    }
}
