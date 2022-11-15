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

package cn.xdf.acdc.connect.hdfs.format.text;

import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemas;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

@Slf4j
public class TextUtil {

    public static final String NULL_VALUE = "\\N";

    public static final String NULL_STRING = "null";

    public static final String BLANK_STRING = " ";

    /**
     * Convert a Struct into a object list.
     *
     * @param struct the struct to convert
     * @return the struct as a object
     */
    public static List<Object> convertStruct(final Struct struct) {
        List<Object> data = new LinkedList<>();
        for (Field field : struct.schema().fields()) {
            // Prioritize empty.
            Object structValue = struct.get(field);
            if (Objects.isNull(structValue) || Objects.equals(structValue, NULL_STRING)) {
                data.add(NULL_VALUE);
            } else {
                Schema schema = field.schema();
                Object value = SinkSchemas.sinkSchemaOf(schema).convertToDbTypeValue(schema, struct.get(field.name()));
                if (value instanceof byte[]) {
                    data.add(convertBytesToString((byte[]) value));
                } else {
                    data.add(value);
                }
            }
        }
        return data;
    }

    private static String convertBytesToString(final byte[] bytes) {
        try {
            return new String(bytes, HdfsSinkConstants.UTF8_CHARACTER);
        } catch (UnsupportedEncodingException e) {
            throw new ConnectException(e);
        }
    }
}
