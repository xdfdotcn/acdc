package cn.xdf.acdc.connect.core.util.config;

import cn.xdf.acdc.connect.core.util.ConfigUtils;
import cn.xdf.acdc.connect.core.util.DeleteEnabledRecommender;
import cn.xdf.acdc.connect.core.util.EnumValidator;
import cn.xdf.acdc.connect.core.util.PrimaryKeyModeRecommender;
import cn.xdf.acdc.connect.core.util.TimeZoneValidator;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

@Getter
@Slf4j
public class SinkConfig extends AbstractConfig {

    public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
            Arrays.asList(
                    "__connect_topic",
                    "__connect_partition",
                    "__connect_offset"
            )
    );

    // CHECKSTYLE:OFF
    protected static final String COMMA = ",";

    protected static final String COLON = ":";

    public static final String DESTINATIONS = "destinations";

    protected static final String DESTINATIONS_DOC =
            "Destination names that the data will be written to.\n"
                    + "It may represents a data table, a topic or etc.\n"
                    + "For example: tb1,tb2";

    protected static final String DESTINATIONS_DISPLAY = "Destinations";

    protected static final String DESTINATIONS_DEFAULT = "";

    public static final String MAX_RETRIES = "max.retries";

    protected static final int MAX_RETRIES_DEFAULT = 3;

    protected static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";

    protected static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";

    protected static final int RETRY_BACKOFF_MS_DEFAULT = 3000;

    protected static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";

    protected static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String BATCH_SIZE = "batch.size";

    protected static final int BATCH_SIZE_DEFAULT = 3000;

    protected static final String BATCH_SIZE_DOC =
            "Specifies how many records to attempt to batch together for insertion into the destination, when possible.";

    protected static final String BATCH_SIZE_DISPLAY = "Batch Size";

    // todo 是否该删除？
    public static final String DELETE_ENABLED = "delete.enabled";

    protected static final String DELETE_ENABLED_DEFAULT = "true";

    protected static final String DELETE_ENABLED_DOC =
            "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
                    + "to be ``record_key``.";

    protected static final String DELETE_ENABLED_DISPLAY = "Enable deletes";

    public static final String PK_FIELDS = "pk.fields";

    protected static final String PK_FIELDS_DEFAULT = "";

    protected static final String PK_FIELDS_DOC =
            "List of comma-separated primary key field names. The runtime interpretation of this config"
                    + " depends on the ``pk.mode``:\n"
                    + "``none``\n"
                    + "    Ignored as no fields are used as primary key in this mode.\n"
                    + "``kafka``\n"
                    + "    Must be a trio representing the Kafka coordinates, defaults to ``"
                    + Joiner.on(COMMA).join(DEFAULT_KAFKA_PK_NAMES) + "`` if empty.\n"
                    + "``record_key``\n"
                    + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
                    + " desired fields - for primitive key only a single field name must be configured.\n"
                    + "``record_value``\n"
                    + "    If empty, all fields from the value struct will be used, otherwise used to extract "
                    + "the desired fields.";

    protected static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

    public static final String PK_MODE = "pk.mode";

    protected static final String PK_MODE_DEFAULT = "record_key";

    protected static final String PK_MODE_DOC =
            "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
                    + "Supported modes are:\n"
                    + "``none``\n"
                    + "    No keys utilized.\n"
                    + "``kafka``\n"
                    + "    Kafka coordinates are used as the PK.\n"
                    + "``record_key``\n"
                    + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
                    + "``record_value``\n"
                    + "    Field(s) from the record value are used, which must be a struct.";

    protected static final String PK_MODE_DISPLAY = "Primary Key Mode";

    protected static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    protected static final String WRITES_GROUP = "Writes";

    protected static final String DATAMAPPING_GROUP = "Data Mapping";

    protected static final String DDL_GROUP = "DDL Support";

    protected static final String RETRIES_GROUP = "Retries";

    protected static final String DESTINATIONS_GROUP = "Destinations";

    public static final String DESTINATIONS_CONFIG_PREFIX = "destinations.";

    public static final String DESTINATIONS_CONFIG_FIELD_WHITELIST = ".fields.whitelist";

    public static final String DESTINATIONS_CONFIG_FIELD_MAPPING = ".fields.mapping";

    public static final String DESTINATIONS_CONFIG_FIELD_ADD = ".fields.add";

    public static final String DESTINATIONS_CONFIG_ROW_FILTER = ".row.filter";

    public static final String DESTINATIONS_CONFIG_DELETE_MODE = ".delete.mode";

    public static final String DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_NAME = ".delete.logical.field.name";

    public static final String DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_VALUE_DELETED = ".delete.logical.field.value.deleted";

    public static final String DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_VALUE_NORMAL = ".delete.logical.field.value.normal";

    public static final String STRING_DEFAULT = "";

    public static final String LOGICAL_DELETE_FIELD_NAME_DEFAULT = "is_delete";

    public static final String LOGICAL_DELETE_FIELD_VALUE_DELETED_DEFAULT = "1";

    public static final String LOGICAL_DELETE_FIELD_VALUE_NORMAL_DEFAULT = "0";

    public static final String DB_TIMEZONE_CONFIG = "db.timezone";

    public static final String DB_TIMEZONE_DEFAULT = "UTC";

    public static final String DB_TIMEZONE_CONFIG_DOC =
            "Name of the timezone that should be used in the connector when "
                    + "inserting time-based values. Defaults to UTC.";

    public static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB Time Zone";

    // CHECKSTYLE:ON
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    MAX_RETRIES,
                    ConfigDef.Type.INT,
                    MAX_RETRIES_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    MAX_RETRIES_DOC,
                    RETRIES_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY
            )
            .define(
                    RETRY_BACKOFF_MS,
                    ConfigDef.Type.INT,
                    RETRY_BACKOFF_MS_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC,
                    RETRIES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    RETRY_BACKOFF_MS_DISPLAY
            )
            // data mapping
            .define(
                    DESTINATIONS,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    DESTINATIONS_DOC,
                    DESTINATIONS_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    DESTINATIONS_DISPLAY
            )
            .define(
                    PK_MODE,
                    ConfigDef.Type.STRING,
                    PK_MODE_DEFAULT,
                    EnumValidator.in(PrimaryKeyMode.values()),
                    ConfigDef.Importance.HIGH,
                    PK_MODE_DOC,
                    DATAMAPPING_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    PK_MODE_DISPLAY,
                    PrimaryKeyModeRecommender.INSTANCE
            )
            .define(
                    PK_FIELDS,
                    ConfigDef.Type.LIST,
                    PK_FIELDS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    PK_FIELDS_DOC,
                    DATAMAPPING_GROUP,
                    2,
                    ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
            )
            .define(
                    BATCH_SIZE,
                    ConfigDef.Type.INT,
                    BATCH_SIZE_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    BATCH_SIZE_DOC,
                    WRITES_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    BATCH_SIZE_DISPLAY
            )
            .define(
                    DELETE_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    DELETE_ENABLED_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    DELETE_ENABLED_DOC,
                    WRITES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    DELETE_ENABLED_DISPLAY,
                    DeleteEnabledRecommender.INSTANCE
            ).define(
                    DB_TIMEZONE_CONFIG,
                    ConfigDef.Type.STRING,
                    DB_TIMEZONE_DEFAULT,
                    TimeZoneValidator.INSTANCE,
                    ConfigDef.Importance.MEDIUM,
                    DB_TIMEZONE_CONFIG_DOC,
                    WRITES_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    DB_TIMEZONE_CONFIG_DISPLAY
            );

    // 成员变量
    private final String connectorName;

    private final int batchSize;

    private final boolean deleteEnabled;

    private final int maxRetries;

    private final int retryBackoffMs;

    private final PrimaryKeyMode pkMode;

    private final List<String> pkFields;

    private final List<String> destinations;

    private final Map<String, DestinationConfig> destinationConfigMapping;

    private final TimeZone timeZone;

    public SinkConfig(final Map<String, String> props) {
        super(CONFIG_DEF, props);
        connectorName = ConfigUtils.connectorName(props);
        batchSize = getInt(BATCH_SIZE);
        deleteEnabled = getBoolean(DELETE_ENABLED);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
        pkFields = getList(PK_FIELDS);
        destinations = getList(DESTINATIONS);
        destinationConfigMapping = parseDestinationConfigs(props);
        String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
        timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
        if (destinationConfigMapping.isEmpty()) {
            throw new ConfigException(
                    "Invalid value for configuration destinations: destination's detail config should be set");
        }

        if (deleteEnabled && pkMode != PrimaryKeyMode.RECORD_KEY) {
            throw new ConfigException(
                    "Primary key mode must be 'record_key' when delete support is enabled");
        }
    }

    private Map<String, DestinationConfig> parseDestinationConfigs(final Map<String, String> props) {
        Preconditions.checkState(destinations != null && !destinations.isEmpty());
        Map<String, DestinationConfig> destinationConfigMapping = new HashMap<>();
        for (String destination : destinations) {
            Set<String> fieldsWhitelist = ConfigUtils.stringToSet(props, DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_FIELD_WHITELIST, COMMA);
            Map<String, String> fieldsMapping = ConfigUtils.stringToMap(props, DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_FIELD_MAPPING, COMMA, COLON);
            Map<String, String> fieldsToAdd = ConfigUtils.stringToMap(props, DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_FIELD_ADD, COMMA, COLON);
            String rowFilterExpress = ConfigUtils.getStringOrDefault(props, DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_ROW_FILTER, STRING_DEFAULT);
            DeleteMode deleteMode = DeleteMode.valueOf(ConfigUtils.getStringOrDefault(props, DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_DELETE_MODE, DeleteMode.PHYSICAL.name()));
            String logicalDeleteFieldName = ConfigUtils.getStringOrDefault(props, DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_NAME,
                    LOGICAL_DELETE_FIELD_NAME_DEFAULT);
            String logicalDeleteFieldValueDeleted = ConfigUtils.getStringOrDefault(props,
                    DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_VALUE_DELETED, LOGICAL_DELETE_FIELD_VALUE_DELETED_DEFAULT);
            String logicalDeleteFieldValueNormal = ConfigUtils.getStringOrDefault(props,
                    DESTINATIONS_CONFIG_PREFIX + destination + DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_VALUE_NORMAL, LOGICAL_DELETE_FIELD_VALUE_NORMAL_DEFAULT);
            DestinationConfig destinationConfig = new DestinationConfig(destination, fieldsWhitelist, fieldsMapping, fieldsToAdd, rowFilterExpress,
                    deleteMode, logicalDeleteFieldName, logicalDeleteFieldValueDeleted, logicalDeleteFieldValueNormal);
            destinationConfigMapping.put(destination, destinationConfig);
        }
        return destinationConfigMapping;
    }

}
