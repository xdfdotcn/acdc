package cn.xdf.acdc.connector.tidb.source;

import cn.xdf.acdc.connector.tidb.TidbConnector;
import cn.xdf.acdc.connector.tidb.util.BigIntUnsignedHandlingMode;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.function.Predicates;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.util.Collect;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Set;
import java.util.function.Predicate;

/**
 * The configuration properties.
 */
public class TidbConnectorConfig extends RelationalDatabaseConnectorConfig {

    /**
     * {@link Integer#MIN_VALUE Minimum value} used for fetch size hint.
     * See <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     */
    public static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;

    /**
     * Set default max partition fetch bytes to 4 mebibyte.
     */
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 4_194_304;

    /**
     * Kafka consumer prefix.
     */
    public static final String KAFKA_CONSUMER_PREFIX = "source.kafka.";

    /**
     * Tidb context name.
     */
    public static final String TIDB_CONTEXT_NAME = "TIDB";

    /**
     * Tidb connector name.
     */
    public static final String TIDB_CONNECTOR_NAME = "tidb";

    /**
     * Built in db names.
     */
    public static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("performance_schema", "sys", "information_schema");

    public static final Field SOURCE_KAFKA_BOOTSTRAP_SERVERS = Field.create("source.kafka.bootstrap.servers")
            .withDisplayName("Tidb source kafka servers")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Ticdc tranfer data from tidb to this kafka.");

    public static final Field SOURCE_KAFKA_TOPIC = Field.create("source.kafka.topic")
            .withDisplayName("Tidb source kafka topic")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Ticdc tranfer data to this kafka topic.");

    public static final Field SOURCE_KAFKA_GROUP_ID = Field.create("source.kafka.group.id")
            .withDisplayName("Tidb source kafka group id")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Consume kafka data by this group.");

    public static final Field SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES = Field.create("source.kafka.max.partition.fetch.bytes")
            .withDisplayName("Tidb source kafka max fetch bytes per partition")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_PARTITION_FETCH_BYTES)
            .withDescription("Consume kafka args: max.partition.fetch.bytes.");

    public static final Field SOURCE_KAFKA_READER_THREAD_NUMBER = Field.create("source.kafka.reader.thread.number")
            .withDisplayName("Tidb source kafka reader thread number")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDefault(1)
            .withDescription("Tidb source kafka reader thread number.");

    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withEnum(TemporalPrecisionMode.class, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
            .withValidation(TidbConnectorConfig::validateTimePrecisionMode)
            .withDescription("Time, date and timestamps can be represented with different kinds of precisions, including:"
                    + "'adaptive_time_microseconds': the precision of date and timestamp values is based the database column's precision; but time fields always use microseconds precision;"
                    + "'connect': always represents time, date and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, "
                    + "which uses millisecond precision regardless of the database columns' precision.");

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE = Field.create("bigint.unsigned.handling.mode")
            .withDisplayName("BIGINT UNSIGNED Handling")
            .withEnum(BigIntUnsignedHandlingMode.class, BigIntUnsignedHandlingMode.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how BIGINT UNSIGNED columns should be represented in change events, including:'precise' uses java.math.BigDecimal to represent values, which are "
                    + "encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; 'long' (the default) represents values "
                    + "using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("TIDB")
            .excluding(
                    SCHEMA_WHITELIST,
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_BLACKLIST,
                    SCHEMA_EXCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE,
                    RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
            .type(
                    SOURCE_KAFKA_BOOTSTRAP_SERVERS,
                    SOURCE_KAFKA_TOPIC,
                    SOURCE_KAFKA_GROUP_ID,
                    SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES,
                    SOURCE_KAFKA_READER_THREAD_NUMBER)
            .connector(
                    BIGINT_UNSIGNED_HANDLING_MODE,
                    TIME_PRECISION_MODE)
            .events(
                    TABLE_IGNORE_BUILTIN,
                    DATABASE_INCLUDE_LIST,
                    DATABASE_EXCLUDE_LIST)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final TemporalPrecisionMode temporalPrecisionMode;

    private final Predicate<String> ddlFilter;

    private final SourceInfoStructMaker<? extends AbstractSourceInfo> sourceInfoStructMaker;

    private final String taskId;

    public TidbConnectorConfig(final Configuration config) {
        super(
                config,
                config.getString(SERVER_NAME),
                TableFilter.fromPredicate(TidbConnectorConfig::isNotBuiltInTable),
                TableId::toString,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.CATALOG);
        taskId = config.getString(TidbConnector.TASK_ID);
        this.temporalPrecisionMode = TemporalPrecisionMode.parse(config.getString(TIME_PRECISION_MODE));

        // Set up the DDL filter
        final String ddlFilter = config.getString(DatabaseHistory.DDL_FILTER);
        this.ddlFilter = (ddlFilter != null) ? Predicates.includes(ddlFilter) : (x -> false);
        this.sourceInfoStructMaker = getSourceInfoStructMaker(Version.parse(config.getString(SOURCE_STRUCT_MAKER_VERSION)));
    }

    /**
     * Validate the time.precision.mode configuration.
     * If {@code adaptive} is specified, this option has the potential to cause overflow which is why the
     * option was deprecated and no longer supported for this connector.
     */
    private static int validateTimePrecisionMode(final Configuration config, final Field field, final ValidationOutput problems) {
        if (config.hasKey(TIME_PRECISION_MODE.name())) {
            final String timePrecisionMode = config.getString(TIME_PRECISION_MODE.name());
            if (TemporalPrecisionMode.ADAPTIVE.getValue().equals(timePrecisionMode)) {
                // this is a problem
                problems.accept(TIME_PRECISION_MODE, timePrecisionMode, "The 'adaptive' time.precision.mode is no longer supported");
                return 1;
            }
        }

        // Everything checks out ok.
        return 0;
    }

    @Override
    public String getContextName() {
        return TIDB_CONTEXT_NAME;
    }

    @Override
    public String getConnectorName() {
        return TIDB_CONNECTOR_NAME;
    }

    /**
     * Get task id.
     * @return task id
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * Get config definition.
     * @return config definition
     */
    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    @Override
    public TemporalPrecisionMode getTemporalPrecisionMode() {
        return temporalPrecisionMode;
    }

    /**
     * Is built in table.
     *
     * @param databaseName database name
     * @return is built in table
     */
    public static boolean isBuiltInDatabase(final String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase());
    }

    /**
     * Is not built in table.
     *
     * @param id table id
     * @return is not built in table
     */
    public static boolean isNotBuiltInTable(final TableId id) {
        return !isBuiltInDatabase(id.catalog());
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(final Version version) {
        return new TidbSourceInfoStructMaker();
    }

    @Override
    public SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker() {
        return sourceInfoStructMaker;
    }
}
