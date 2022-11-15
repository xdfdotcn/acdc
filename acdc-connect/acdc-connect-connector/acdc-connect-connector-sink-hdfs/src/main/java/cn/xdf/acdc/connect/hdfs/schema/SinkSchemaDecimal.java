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

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;

public final class SinkSchemaDecimal implements SinkSchema {

    public static final Pattern DECIMAL_PATTERN = Pattern.compile("(^[a-zA-Z]+)(\\()(\\d+)(,)(\\d+)(\\))");

    private static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

    private static final int HIVE_DECIMAL_PRECISION_MAX = 38;

    private static final SinkSchema INSTANCE = new SinkSchemaDecimal();

    /**
     * Get the singleton instance.
     * @return SinkSchema
     */
    public static SinkSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return "decimal";
    }

    @Override
    public Schema schemaOf(final String sinkDataTypeName) {
        Matcher m = DECIMAL_PATTERN.matcher(sinkDataTypeName);
        if (!m.matches()) {
            throw new ConfigException("Unknown type name");
        }
        int scale = Integer.valueOf(m.group(5));
        return Decimal.builder(scale)
            .optional()
            .parameter(SinkSchemas.DATA_TYPE_NAME_KEY, sinkDataTypeName)
            .parameter(SinkSchemas.NAME_KEY, name())
            .build();
    }

    @Override
    public boolean isPromotable(final Type schema) {
        return Type.BYTES == schema;
    }

    @Override
    public boolean isCompatibility(final Schema source) {
        return Objects.equals(Decimal.LOGICAL_NAME, source.name());
    }

    @Override
    public Object convertToJavaTypeValue(final Schema source, final Object recordValue) {
        return recordValue;
    }

    @Override
    public Object convertToDbTypeValue(final Schema source, final Object recordValue) {
        return recordValue;
    }

    @Override
    public String sinkDataTypeNameOf(final Schema schema) {

        String scale = schema.parameters().get(Decimal.SCALE_FIELD);
        String precision = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
        if (precision != null && Integer.parseInt(precision) > HIVE_DECIMAL_PRECISION_MAX) {
            throw new ConnectException(
                String.format("Illegal precision %s : Hive allows at most %d precision.",
                    precision,
                    HIVE_DECIMAL_PRECISION_MAX)
            );
        }
        // Let precision always be HIVE_DECIMAL_PRECISION_MAX. Hive serde will try the best
        // to fit decimal data into decimal schema. If the data is too long even for
        // the maximum precision, hive will throw serde exception. No data loss risk.
        return new DecimalTypeInfo(HIVE_DECIMAL_PRECISION_MAX, Integer.parseInt(scale)).getTypeName();
    }
}
