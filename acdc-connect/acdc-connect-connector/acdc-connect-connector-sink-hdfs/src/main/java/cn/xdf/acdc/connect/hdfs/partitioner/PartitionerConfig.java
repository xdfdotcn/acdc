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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cn.xdf.acdc.connect.hdfs.common.ComposableConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.joda.time.DateTimeZone;

public class PartitionerConfig extends AbstractConfig implements ComposableConfig {

    // Partitioner group
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";

    public static final String PARTITIONER_CLASS_DOC =
        "The partitioner to use when writing data to the store. You can use ``DefaultPartitioner``, "
            + "which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to "
            + "different directories according to the value of the partitioning field specified "
            + "in ``partition.field.name``; ``TimeBasedPartitioner``, which partitions data "
            + "according to ingestion time.";

    public static final Class<?> PARTITIONER_CLASS_DEFAULT = DefaultPartitioner.class;

    public static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

    public static final String PARTITION_FIELD_NAME_CONFIG = "partition.field.name";

    public static final String PARTITION_FIELD_NAME_DOC =
        "The name of the partitioning field when FieldPartitioner is used.";

    public static final String PARTITION_FIELD_NAME_DEFAULT = "";

    public static final String PARTITION_FIELD_NAME_DISPLAY = "Partition Field Name";

    public static final String PARTITION_DURATION_MS_CONFIG = "partition.duration.ms";

    public static final String PARTITION_DURATION_MS_DOC =
        "The duration of a partition milliseconds used by ``TimeBasedPartitioner``. "
            + "The default value -1 means that we are not using ``TimeBasedPartitioner``.";

    public static final long PARTITION_DURATION_MS_DEFAULT = -1L;

    public static final String PARTITION_DURATION_MS_DISPLAY = "Partition Duration (ms)";

    public static final String PATH_FORMAT_CONFIG = "path.format";

    public static final String PATH_FORMAT_DOC =
        "This configuration is used to set the format of the data directories when partitioning with "
            + "``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp"
            + " to proper directories strings. For example, if you set "
            + "``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH``, the data directories will have"
            + " the format ``/year=2015/month=12/day=07/hour=15/``.";

    public static final String PATH_FORMAT_DEFAULT = "";

    public static final String PATH_FORMAT_DISPLAY = "Path Format";

    public static final String LOCALE_CONFIG = "locale";

    public static final String LOCALE_DOC =
        "The locale to use when partitioning with ``TimeBasedPartitioner``. Used to format dates "
            + "and times. For example, use ``en-US`` for US English, ``en-GB`` for UK English, or "
            + "``fr-FR`` for French (in France). These may vary by Java version. See the `available"
            + " locales <http://www.localeplanet.com/java/>`__.";

    public static final String LOCALE_DEFAULT = "";

    public static final String LOCALE_DISPLAY = "Locale";

    public static final String TIMEZONE_CONFIG = "timezone";

    public static final String TIMEZONE_DOC =
        "The timezone to use when partitioning with ``TimeBasedPartitioner``. Used to format and "
            + "compute dates and times. All timezone IDs must be specified in the long format, such as "
            + "``America/Los_Angeles``, ``America/New_York``, and ``Europe/Paris``, or ``UTC``. "
            + "Alternatively a locale independent, fixed offset, datetime zone can be specified in form "
            + "``[+-]hh:mm``. Support for these timezones "
            + "may vary by Java version. See the `available timezones within each locale "
            + "<http://www.localeplanet.com/java/>`__, such as `those within the US English locale "
            + "<http://www.localeplanet.com/java/en-US/index.html>`__.";

    public static final String TIMEZONE_DEFAULT = "";

    public static final String TIMEZONE_DISPLAY = "Timezone";

    public static final String TIMESTAMP_EXTRACTOR_CLASS_CONFIG = "timestamp.extractor";

    public static final String TIMESTAMP_EXTRACTOR_CLASS_DOC = "The extractor that gets the "
        + "timestamp for records when partitioning with ``TimeBasedPartitioner``. It can be set to "
        + "``Wallclock``, ``Record`` or ``RecordField`` in order to use one of the built-in "
        + "timestamp extractors or be given the fully-qualified class name of a user-defined class "
        + "that extends the ``TimestampExtractor`` interface.";

    public static final String TIMESTAMP_EXTRACTOR_CLASS_DEFAULT = "Wallclock";

    public static final String TIMESTAMP_EXTRACTOR_CLASS_DISPLAY = "Timestamp Extractor";

    public static final String TIMESTAMP_FIELD_NAME_CONFIG = "timestamp.field";

    public static final String TIMESTAMP_FIELD_NAME_DOC =
        "The record field to be used as timestamp by the timestamp extractor.";

    public static final String TIMESTAMP_FIELD_NAME_DEFAULT = "timestamp";

    public static final String TIMESTAMP_FIELD_NAME_DISPLAY = "Record Field for Timestamp Extractor";

    public PartitionerConfig(final ConfigDef configDef, final Map<String, String> props) {
        super(configDef, props);
    }

    /**
     * Create a new configuration definition.
     *
     * @param partitionerClassRecommender A recommender for partitioner classes shipping
     *     out-of-the-box with a connector. The recommender should not prevent additional custom
     *     classes from being added during runtime.
     * @return the newly created configuration definition.
     */
    public static ConfigDef newConfigDef(final ConfigDef.Recommender partitionerClassRecommender) {
        ConfigDef configDef = new ConfigDef();
        // Define Partitioner configuration group
        final String group = "Partitioner";
        int orderInGroup = 0;

        configDef.define(PARTITIONER_CLASS_CONFIG,
            Type.CLASS,
            PARTITIONER_CLASS_DEFAULT,
            Importance.HIGH,
            PARTITIONER_CLASS_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            PARTITIONER_CLASS_DISPLAY,
            Arrays.asList(
                PARTITION_FIELD_NAME_CONFIG,
                PARTITION_DURATION_MS_CONFIG,
                PATH_FORMAT_CONFIG,
                LOCALE_CONFIG,
                TIMEZONE_CONFIG
            ),
            partitionerClassRecommender);

        configDef.define(PARTITION_FIELD_NAME_CONFIG,
            Type.LIST,
            PARTITION_FIELD_NAME_DEFAULT,
            Importance.MEDIUM,
            PARTITION_FIELD_NAME_DOC,
            group,
            ++orderInGroup,
            Width.NONE,
            PARTITION_FIELD_NAME_DISPLAY,
            new PartitionerClassDependentsRecommender());

        configDef.define(PARTITION_DURATION_MS_CONFIG,
            Type.LONG,
            PARTITION_DURATION_MS_DEFAULT,
            Importance.MEDIUM,
            PARTITION_DURATION_MS_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            PARTITION_DURATION_MS_DISPLAY,
            new PartitionerClassDependentsRecommender());

        configDef.define(PATH_FORMAT_CONFIG,
            Type.STRING,
            PATH_FORMAT_DEFAULT,
            Importance.MEDIUM,
            PATH_FORMAT_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            PATH_FORMAT_DISPLAY,
            new PartitionerClassDependentsRecommender());

        configDef.define(LOCALE_CONFIG,
            Type.STRING,
            LOCALE_DEFAULT,
            Importance.MEDIUM,
            LOCALE_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            LOCALE_DISPLAY,
            new PartitionerClassDependentsRecommender());

        configDef.define(TIMEZONE_CONFIG,
            Type.STRING,
            TIMEZONE_DEFAULT,
            new TimezoneValidator(),
            Importance.MEDIUM,
            TIMEZONE_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            TIMEZONE_DISPLAY,
            new PartitionerClassDependentsRecommender());

        configDef.define(TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            Type.STRING,
            TIMESTAMP_EXTRACTOR_CLASS_DEFAULT,
            Importance.MEDIUM,
            TIMESTAMP_EXTRACTOR_CLASS_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            TIMESTAMP_EXTRACTOR_CLASS_DISPLAY);

        configDef.define(TIMESTAMP_FIELD_NAME_CONFIG,
            Type.STRING,
            TIMESTAMP_FIELD_NAME_DEFAULT,
            Importance.MEDIUM,
            TIMESTAMP_FIELD_NAME_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            TIMESTAMP_FIELD_NAME_DISPLAY);

        return configDef;
    }

    private static boolean classNameEquals(final Class<?> left, final Class<?> right) {
        return left.getName().equals(right.getName())
            || left.getSimpleName().equals(right.getSimpleName());
    }

    @Override
    public Object get(final String key) {
        return super.get(key);
    }

    public static class BooleanParentRecommender implements ConfigDef.Recommender {

        private final String parentConfigName;

        public BooleanParentRecommender(final String parentConfigName) {
            this.parentConfigName = parentConfigName;
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            return (boolean) connectorConfigs.get(parentConfigName);
        }
    }

    public static class PartitionerClassDependentsRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> props) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Partitioner<?>> partitioner =
                    (Class<? extends Partitioner<?>>) connectorConfigs.get(PARTITIONER_CLASS_CONFIG);
                if (classNameEquals(DefaultPartitioner.class, partitioner)) {
                    return false;
                } else if (FieldPartitioner.class.isAssignableFrom(partitioner)) {
                    // subclass of FieldPartitioner
                    return name.equals(PARTITION_FIELD_NAME_CONFIG);
                } else if (TimeBasedPartitioner.class.isAssignableFrom(partitioner)) {
                    // subclass of TimeBasedPartitioner
                    if (classNameEquals(DailyPartitioner.class, partitioner)
                        || classNameEquals(HourlyPartitioner.class, partitioner)) {
                        return name.equals(LOCALE_CONFIG) || name.equals(TIMEZONE_CONFIG);
                    } else {
                        return name.equals(PARTITION_DURATION_MS_CONFIG)
                            || name.equals(PATH_FORMAT_CONFIG)
                            || name.equals(LOCALE_CONFIG)
                            || name.equals(TIMEZONE_CONFIG);
                    }
                } else {
                    // Custom partitioner. Allow all the dependent configs.
                    return true;
                }
            } catch (ClassCastException e) {
                ConfigException ce = new ConfigException(
                    "Partitioner class not found: "
                        + PARTITIONER_CLASS_CONFIG
                );
                ce.initCause(e);
                throw ce;
            }
        }
    }

    public static class TimezoneValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(final String name, final Object timezone) {
            String timezoneStr = ((String) timezone).trim();
            if (!timezoneStr.isEmpty()) {
                try {
                    DateTimeZone.forID(timezoneStr);
                } catch (IllegalArgumentException e) {
                    throw new ConfigException(
                        name,
                        timezone,
                        e.getMessage()
                    );
                }
            }
        }

        @Override
        public String toString() {
            return "Any timezone accepted by: " + DateTimeZone.class;
        }
    }
}
