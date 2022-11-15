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

package cn.xdf.acdc.connect.hdfs.partitioner;

import cn.xdf.acdc.connect.hdfs.common.SchemaGenerator;
import cn.xdf.acdc.connect.hdfs.common.StorageCommonConfig;
import cn.xdf.acdc.connect.hdfs.partitioner.errors.PartitionException;
import cn.xdf.acdc.connect.hdfs.util.DataUtils;
import com.google.common.base.Strings;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

@Slf4j
public class TimeBasedPartitioner<T> extends DefaultPartitioner<T> {

    private static final String SCHEMA_GENERATOR_CLASS =
        "cn.xdf.acdc.connect.hdfs.hive.schema.TimeBasedSchemaGenerator";

    private static final Pattern NUMERIC_TIMESTAMP_PATTERN = Pattern.compile("^-?[0-9]{1,19}$");

    // Duration of a partition in milliseconds.
    private long partitionDurationMs;

    private String pathFormat;

    private DateTimeFormatter formatter;

    private TimestampExtractor timestampExtractor;

    protected void init(
        final long partitionDurationMs,
        final String pathFormat,
        final Locale locale,
        final DateTimeZone timeZone,
        final Map<String, Object> config
    ) {
        setDelim((String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG));
        this.partitionDurationMs = partitionDurationMs;
        this.pathFormat = pathFormat;
        try {
            this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
            timestampExtractor = newTimestampExtractor(
                (String) config.get(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
            timestampExtractor.configure(config);
        } catch (IllegalArgumentException e) {
            ConfigException ce = new ConfigException(
                PartitionerConfig.PATH_FORMAT_CONFIG,
                pathFormat,
                e.getMessage()
            );
            ce.initCause(e);
            throw ce;
        }
    }

    private static DateTimeFormatter getDateTimeFormatter(final String str, final DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    /**
     * Get partition.
     * @param timeGranularityMs time granularity
     * @param timestamp cur timestamp
     * @param timeZone time zone
     * @return the value of partition
     */
    public static long getPartition(long timeGranularityMs, long timestamp, final DateTimeZone timeZone) {
        long adjustedTimestamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = (adjustedTimestamp / timeGranularityMs) * timeGranularityMs;
        return timeZone.convertLocalToUTC(partitionedTime, false);
    }

    /**
     * Partition dir path format .
     * @return the path format
     */
    public String getPathFormat() {
        return pathFormat;
    }

    /**
     * public for testing.
     * @return TimestampExtractor
     */
    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }

    @Override
    public void configure(final Map<String, Object> config) {
        super.configure(config);
        long partitionDurationMsProp =
            (long) config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
        if (partitionDurationMsProp < 0) {
            throw new ConfigException(
                PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
                partitionDurationMsProp,
                "Partition duration needs to be a positive."
            );
        }

        String pathFormat = (String) config.get(PartitionerConfig.PATH_FORMAT_CONFIG);
        if (Strings.isNullOrEmpty(pathFormat) || pathFormat.equals(getDelim())) {
            throw new ConfigException(
                PartitionerConfig.PATH_FORMAT_CONFIG,
                pathFormat,
                "Path format cannot be empty."
            );
        } else if (!Strings.isNullOrEmpty(getDelim()) && pathFormat.endsWith(getDelim())) {
            // Delimiter has been added by the user at the end of the path format string. Removing.
            pathFormat = pathFormat.substring(0, pathFormat.length() - getDelim().length());
        }

        String localeString = (String) config.get(PartitionerConfig.LOCALE_CONFIG);
        if (Strings.isNullOrEmpty(localeString)) {
            throw new ConfigException(
                PartitionerConfig.LOCALE_CONFIG,
                localeString,
                "Locale cannot be empty."
            );
        }

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        if (Strings.isNullOrEmpty(timeZoneString)) {
            throw new ConfigException(
                PartitionerConfig.TIMEZONE_CONFIG,
                timeZoneString,
                "Timezone cannot be empty."
            );
        }

        Locale locale = new Locale(localeString);
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
        init(partitionDurationMsProp, pathFormat, locale, timeZone, config);
    }

    @Override
    public String encodePartition(final SinkRecord sinkRecord, long nowInMillis) {
        Long timestamp = timestampExtractor.extract(sinkRecord, nowInMillis);
        return encodedPartitionForTimestamp(sinkRecord, timestamp);
    }

    @Override
    public String encodePartition(final SinkRecord sinkRecord) {
        Long timestamp = timestampExtractor.extract(sinkRecord);
        return encodedPartitionForTimestamp(sinkRecord, timestamp);
    }

    private String encodedPartitionForTimestamp(final SinkRecord sinkRecord, final Long timestamp) {
        if (timestamp == null) {
            String msg = "Unable to determine timestamp using timestamp.extractor "
                + timestampExtractor.getClass().getName()
                + " for record: "
                + sinkRecord;
            log.error(msg);
            throw new PartitionException(msg);
        }
        DateTime bucket = new DateTime(
            getPartition(partitionDurationMs, timestamp, formatter.getZone())
        );
        return bucket.toString(formatter);
    }

    @Override
    public List<T> partitionFields() {
        if (getPartitionFields() == null) {
            setPartitionFields(newSchemaGenerator(getConfig()).newPartitionFields(pathFormat));
        }
        return getPartitionFields();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Class<? extends SchemaGenerator<T>> getSchemaGeneratorClass()
        throws ClassNotFoundException {
        return (Class<? extends SchemaGenerator<T>>) Class.forName(SCHEMA_GENERATOR_CLASS);
    }

    /**
     * Create a timestamp extractor by class type.
     * @param extractClassType  class type
     * @return the timestamp extractor
     */
    public TimestampExtractor newTimestampExtractor(final String extractClassType) {
        String extractorClassName = "";
        try {
            switch (extractClassType) {
                case "Wallclock":
                case "Record":
                case "RecordField":
                    extractorClassName = "cn.xdf.acdc.connect.hdfs.partitioner.TimeBasedPartitioner$"
                        + extractClassType
                        + "TimestampExtractor";
                    break;
                default:
                    extractorClassName = extractClassType;
                    break;
            }
            Class<?> klass = Class.forName(extractorClassName);
            if (!TimestampExtractor.class.isAssignableFrom(klass)) {
                throw new ConnectException(
                    "Class " + extractorClassName + " does not implement TimestampExtractor"
                );
            }
            return (TimestampExtractor) klass.newInstance();
        } catch (ClassNotFoundException
            | ClassCastException
            | IllegalAccessException
            | InstantiationException e) {
            ConfigException ce = new ConfigException(
                "Invalid timestamp extractor: " + extractorClassName
            );
            ce.initCause(e);
            throw ce;
        }
    }

    public static class WallclockTimestampExtractor implements TimestampExtractor {

        @Override
        public void configure(final Map<String, Object> config) {
        }

        /**
         * Returns the current timestamp supplied by the caller, which is assumed to be the processing
         * time.
         *
         * @param record Record from which to extract time
         * @param nowInMillis Time in ms specified by caller, useful for getting consistent wallclocks
         * @return The wallclock specified by the input parameter in milliseconds
         */
        @Override
        public Long extract(final ConnectRecord<?> record, long nowInMillis) {
            return nowInMillis;
        }

        /**
         * Returns the current time from {@link Time#SYSTEM}.
         *
         * @param record Record to extract time from
         * @return Wallclock time in milliseconds
         */
        @Override
        public Long extract(final ConnectRecord<?> record) {
            return Time.SYSTEM.milliseconds();
        }
    }

    public static class RecordTimestampExtractor implements TimestampExtractor {

        @Override
        public void configure(final Map<String, Object> config) {
        }

        @Override
        public Long extract(final ConnectRecord<?> record) {
            return record.timestamp();
        }
    }

    public static class RecordFieldTimestampExtractor implements
        TimestampExtractor {

        private String fieldName;

        private DateTimeFormatter dateTime;

        @Override
        public void configure(final Map<String, Object> config) {
            fieldName = (String) config.get(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG);
            dateTime = ISODateTimeFormat.dateTimeParser();
        }

        @Override
        public Long extract(final ConnectRecord<?> record) {
            Object value = record.value();
            if (value instanceof Struct) {
                Struct struct = (Struct) value;
                Object timestampValue = DataUtils.getNestedFieldValue(struct, fieldName);
                Schema fieldSchema = DataUtils.getNestedField(record.valueSchema(), fieldName).schema();

                if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
                    return ((Date) timestampValue).getTime();
                }

                switch (fieldSchema.type()) {
                    case INT32:
                    case INT64:
                        return ((Number) timestampValue).longValue();
                    case STRING:
                        return extractTimestampFromString((String) timestampValue);
                    default:
                        log.error(
                            "Unsupported type '{}' for user-defined timestamp field.",
                            fieldSchema.type().getName()
                        );
                        throw new PartitionException(
                            "Error extracting timestamp from record field: " + fieldName
                        );
                }
            } else if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                Object timestampValue = DataUtils.getNestedFieldValue(map, fieldName);
                if (timestampValue instanceof Number) {
                    return ((Number) timestampValue).longValue();
                } else if (timestampValue instanceof String) {
                    return extractTimestampFromString((String) timestampValue);
                } else if (timestampValue instanceof Date) {
                    return ((Date) timestampValue).getTime();
                } else {
                    log.error(
                        "Unsupported type '{}' for user-defined timestamp field.",
                        timestampValue.getClass()
                    );
                    throw new PartitionException(
                        "Error extracting timestamp from record field: " + fieldName
                    );
                }
            } else {
                log.error("Value is not of Struct or Map type.");
                throw new PartitionException("Error encoding partition.");
            }
        }

        private Long extractTimestampFromString(final String timestampValue) {
            if (NUMERIC_TIMESTAMP_PATTERN.matcher(timestampValue).matches()) {
                try {
                    return Long.valueOf(timestampValue);
                } catch (NumberFormatException e) {
                    // expected, ignore
                }
            }
            return dateTime.parseMillis(timestampValue);
        }
    }
}
