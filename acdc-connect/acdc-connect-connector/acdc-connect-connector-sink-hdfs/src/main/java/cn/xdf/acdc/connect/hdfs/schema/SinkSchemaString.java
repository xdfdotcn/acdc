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

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.hdfs.util.DateTimeUtils;
import com.google.common.base.Strings;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class SinkSchemaString implements SinkSchema {

    private static final String DATE_FORMATTER = "yyyy-MM-dd";

    private static final String TIME_FORMATTER = "HH:mm:ss";

    private static final String TIMESTAMP_FORMATTER = "yyyy-MM-dd HH:mm:ss";

    private static final SinkSchema INSTANCE = new SinkSchemaString();

    private final DateTimeFormatter dateFormatter;

    private final DateTimeFormatter timeFormatter;

    private final DateTimeFormatter timestampFormatter;

    public SinkSchemaString() {
        ZoneId zoneId = ZoneId.systemDefault();
        dateFormatter = DateTimeFormatter.ofPattern(DATE_FORMATTER).withZone(zoneId);
        timeFormatter = DateTimeFormatter.ofPattern(TIME_FORMATTER).withZone(zoneId);
        timestampFormatter = DateTimeFormatter.ofPattern(TIMESTAMP_FORMATTER).withZone(zoneId);
    }

    /**
     * Get the singleton instance.
     * @return SinkSchema
     */
    public static SinkSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return "string";
    }

    @Override
    public Schema schemaOf(final String sinkDataTypeName) {
        return SchemaBuilder.string()
            .optional()
            .parameter(SinkSchemas.DATA_TYPE_NAME_KEY, sinkDataTypeName)
            .parameter(SinkSchemas.NAME_KEY, name())
            .build();
    }

    @Override
    public boolean isPromotable(final Type schema) {
        return true;
    }

    @Override
    public boolean isCompatibility(final Schema source) {
        return true;
    }

    @Override
    public Object convertToJavaTypeValue(final Schema source, final Object recordValue) {
        if (!Objects.equals(Decimal.LOGICAL_NAME, source.name()) && Type.BYTES == source.type()) {
            return convertBytesToString(source, recordValue);
        }
        if (Strings.isNullOrEmpty(source.name())) {
            return String.valueOf(recordValue);
        }
        // Date,Timestamp,Time,ZonedTimestamp
        switch (source.name()) {
            case Date.LOGICAL_NAME:
                return formatDate(convertToInstant(recordValue), dateFormatter);
            case Timestamp.LOGICAL_NAME:
                return formatDate(convertToInstant(recordValue), timestampFormatter);
            case Time.LOGICAL_NAME:
                return formatDate(convertToInstant(recordValue), timeFormatter);
            case ZonedTimestamp.LOGICAL_NAME:
                Instant instant = DateTimeUtils.verifyDateFormatAndGetDate((String) recordValue).toInstant();
                return formatDate(instant, timestampFormatter);
            // Decimal,other...
            default:
                return String.valueOf(recordValue);
        }
    }

    @Override
    public Object convertToDbTypeValue(final Schema source, final Object recordValue) {
        if (Strings.isNullOrEmpty(source.name()) && Type.BYTES == source.type()) {
            return convertBytesToString(source, recordValue);
        }
        return String.valueOf(recordValue);
    }

    @Override
    public String sinkDataTypeNameOf(final Schema schema) {
        return name();
    }

    private Instant convertToInstant(final Object record) {
        return DateTimeUtils.fixTimeZone((java.util.Date) record).toInstant();
    }

    private String formatDate(final Instant instant, final DateTimeFormatter formatter) {
        return formatter.format(instant);
    }
}
