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

package cn.xdf.acdc.connect.hdfs.storage;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.hdfs.common.ComposableConfig;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StorageSinkConnectorConfig extends SinkConfig implements ComposableConfig {

    // Connector group
    public static final String FLUSH_SIZE_CONFIG = "flush.size";

    public static final String FLUSH_SIZE_DOC =
        "Number of records written to store before invoking file commits.";

    public static final String FLUSH_SIZE_DISPLAY = "Flush Size";

    public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";

    public static final String
        ROTATE_INTERVAL_MS_DOC =
        "The time interval in milliseconds to invoke file commits. You can configure this parameter"
            + " so that the time interval is determined by using a timestamp extractor (for "
            + "example, Kafka Record Time, Record Field, or Wall Clock extractor). When the first "
            + "record is processed, a timestamp is set as the base time. This is useful if you "
            + "require exactly-once-semantics. This configuration ensures that file commits are "
            + "invoked at every configured interval. The default value ``-1`` indicates that this "
            + "feature is disabled.";

    public static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;

    public static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

    public static final String ROTATE_SCHEDULE_INTERVAL_MS_CONFIG = "rotate.schedule.interval.ms";

    public static final String ROTATE_SCHEDULE_INTERVAL_MS_DOC =
        "The time interval in milliseconds to periodically invoke file commits. This configuration "
            + "ensures that file commits are invoked at every configured interval. Time of commit "
            + "will be adjusted to 00:00 of selected timezone. The commit will be performed at the "
            + "scheduled time, regardless of the previous commit time or number of messages. This "
            + "configuration is useful when you have to commit your data based on current server "
            + "time, for example at the beginning of every hour. The default value ``-1`` means "
            + "that this feature is disabled.";

    public static final long ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT = -1L;

    public static final String ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY = "Rotate Schedule Interval (ms)";

    public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";

    public static final String
        SHUTDOWN_TIMEOUT_DOC =
        "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
            + "completed during connector shutdown.";

    public static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;

    public static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG =
        "filename.offset.zero.pad.width";

    public static final String
        FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
        "Width to zero pad offsets in store's filenames if offsets are too short in order to "
            + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";

    public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;

    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY =
        "Filename Offset Zero Pad Width";

    public static final String SCHEMA_CACHE_SIZE_CONFIG = AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;

    public static final String SCHEMA_CACHE_SIZE_DOC =
        "The size of the schema cache used in the Avro converter.";

    public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;

    public static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG = "enhanced.avro.schema.support";

    public static final boolean ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT = true;

    public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DOC =
        "Enable enhanced avro schema support in AvroConverter: Enum symbol preservation and Package"
            + " Name awareness";

    public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DISPLAY = "Enhanced Avro Support";

    public static final String CONNECT_META_DATA_CONFIG = "connect.meta.data";

    public static final boolean CONNECT_META_DATA_DEFAULT = true;

    public static final String CONNECT_META_DATA_DOC =
        "Allow connect converter to add its metadata to the output schema";

    public static final String CONNECT_META_DATA_DISPLAY = "Connect Metadata";

    public static final String AVRO_CODEC_CONFIG = "avro.codec";

    public static final String AVRO_CODEC_DEFAULT = "null";

    public static final String AVRO_CODEC_DISPLAY = "Avro Compression Codec";

    public static final String AVRO_CODEC_DOC = "The Avro compression codec to be used for output  "
        + "files. Available values: null, deflate, snappy and bzip2 (CodecSource is org.apache"
        + ".avro.file.CodecFactory)";

    public static final String[] AVRO_SUPPORTED_CODECS = new String[] {"null", "deflate", "snappy",
        "bzip2"};

    public static final String PARQUET_CODEC_CONFIG = "parquet.codec";

    public static final String PARQUET_CODEC_DEFAULT = "snappy";

    public static final String PARQUET_CODEC_DISPLAY = "Parquet Compression Codec";

    public static final String PARQUET_CODEC_DOC = "The Parquet compression codec to be used for "
        + "output files.";

    // Schema group
    public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";

    public static final String SCHEMA_COMPATIBILITY_DOC =
        "The schema compatibility rule to use when the connector is observing schema changes. The "
            + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";

    public static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";

    public static final String SCHEMA_COMPATIBILITY_DISPLAY = "Schema Compatibility";

    // CHECKSTYLE:OFF
    public static final ConfigDef.Recommender schemaCompatibilityRecommender =
        new SchemaCompatibilityRecommender();
    // CHECKSTYLE:ON

    public StorageSinkConnectorConfig(final Map<String, String> props) {
        super(props);
    }

    /**
     * Create a new configuration definition.
     *  a connector. The recommender should not prevent additional custom classes from being
     *  added during runtime.
     * @return the newly created configuration definition.
     */
    public static ConfigDef newConfigDef() {
        ConfigDef configDef = new ConfigDef();
        // Define Store's basic configuration group
        final String group = "Connector";
        int orderInGroup = 0;
        configDef.define(
            FLUSH_SIZE_CONFIG,
            Type.INT,
            Importance.HIGH,
            FLUSH_SIZE_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            FLUSH_SIZE_DISPLAY
        );

        configDef.define(
            ROTATE_INTERVAL_MS_CONFIG,
            Type.LONG,
            ROTATE_INTERVAL_MS_DEFAULT,
            Importance.HIGH,
            ROTATE_INTERVAL_MS_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            ROTATE_INTERVAL_MS_DISPLAY
        );

        configDef.define(
            ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
            Type.LONG,
            ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT,
            Importance.MEDIUM,
            ROTATE_SCHEDULE_INTERVAL_MS_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY
        );

        configDef.define(
            SCHEMA_CACHE_SIZE_CONFIG,
            Type.INT,
            SCHEMA_CACHE_SIZE_DEFAULT,
            Importance.LOW,
            SCHEMA_CACHE_SIZE_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            SCHEMA_CACHE_SIZE_DISPLAY
        );

        configDef.define(
            ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
            Type.BOOLEAN,
            ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT,
            Importance.LOW,
            ENHANCED_AVRO_SCHEMA_SUPPORT_DOC,
            group,
            ++orderInGroup,
            Width.SHORT,
            ENHANCED_AVRO_SCHEMA_SUPPORT_DISPLAY
        );

        configDef.define(
            CONNECT_META_DATA_CONFIG,
            Type.BOOLEAN,
            CONNECT_META_DATA_DEFAULT,
            Importance.LOW,
            CONNECT_META_DATA_DOC,
            group,
            ++orderInGroup,
            Width.SHORT,
            CONNECT_META_DATA_DISPLAY
        );

        configDef.define(
            SHUTDOWN_TIMEOUT_CONFIG,
            Type.LONG,
            SHUTDOWN_TIMEOUT_DEFAULT,
            Importance.MEDIUM,
            SHUTDOWN_TIMEOUT_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            SHUTDOWN_TIMEOUT_DISPLAY
        );

        configDef.define(
            FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG,
            Type.INT,
            FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT,
            ConfigDef.Range.atLeast(0),
            Importance.LOW,
            FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY
        );

        configDef.define(
            AVRO_CODEC_CONFIG,
            Type.STRING,
            AVRO_CODEC_DEFAULT,
            ConfigDef.ValidString.in(AVRO_SUPPORTED_CODECS),
            Importance.LOW,
            AVRO_CODEC_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            AVRO_CODEC_DISPLAY
        );

        // Define Schema configuration group
        final String schemaGroup = "Schema";
        int orderInSchemaGroup = 0;
        // Define Schema configuration group
        configDef.define(
            SCHEMA_COMPATIBILITY_CONFIG,
            Type.STRING,
            SCHEMA_COMPATIBILITY_DEFAULT,
            Importance.HIGH,
            SCHEMA_COMPATIBILITY_DOC,
            schemaGroup,
            ++orderInSchemaGroup,
            Width.SHORT,
            SCHEMA_COMPATIBILITY_DISPLAY,
            schemaCompatibilityRecommender
        );
        return configDef;
    }

    /**
     * Get avro code deserialization.
     * @return the deserialization
     */
    public String getAvroCodec() {
        return getString(AVRO_CODEC_CONFIG);
    }

    @Override
    public Object get(final String key) {
        return super.get(key);
    }

    /**
     * Get avro data config .
     * @return the avro data config
     */
    public AvroDataConfig avroDataConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(SCHEMA_CACHE_SIZE_CONFIG, get(SCHEMA_CACHE_SIZE_CONFIG));
        props.put(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, get(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
        props.put(CONNECT_META_DATA_CONFIG, get(CONNECT_META_DATA_CONFIG));
        return new AvroDataConfig(props);
    }

    /**
     * Add parquet codec configuration to enable compression options for storage sink connectors
     * that support Parquet format.
     *
     * @param configDef The configuration definition to be extended with the parquet codec property
     * @param parquetRecommender A recommender and validator for parquet compression codecs
     * @param group The initial position order in the group
     * @param initialOrder The initial position order in the group
     * @param <T> The recommender type
     */
    public static <T extends ConfigDef.Recommender & ConfigDef.Validator> void enableParquetConfig(
        final ConfigDef configDef,
        final T parquetRecommender,
        final String group,
        int initialOrder
    ) {
        int orderInGroup = initialOrder;
        configDef.define(
            PARQUET_CODEC_CONFIG,
            Type.STRING,
            PARQUET_CODEC_DEFAULT,
            parquetRecommender,
            Importance.LOW,
            PARQUET_CODEC_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            PARQUET_CODEC_DISPLAY,
            parquetRecommender
        );
    }

    public static class SchemaCompatibilityRecommender extends BooleanParentRecommender {

        public SchemaCompatibilityRecommender() {
            super("hive.integration");
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> connectorConfigs) {
            Boolean hiveIntegration = (Boolean) connectorConfigs.get(getParentConfigName());
            if (hiveIntegration != null && hiveIntegration) {
                return Arrays.asList("BACKWARD", "FORWARD", "FULL");
            } else {
                return Arrays.asList("NONE", "BACKWARD", "FORWARD", "FULL");
            }
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> connectorConfigs) {
            return true;
        }
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

        protected String getParentConfigName() {
            return parentConfigName;
        }
    }
}
