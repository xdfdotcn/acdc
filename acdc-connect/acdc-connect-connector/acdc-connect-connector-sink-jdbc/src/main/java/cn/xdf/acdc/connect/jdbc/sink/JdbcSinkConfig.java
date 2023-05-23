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

package cn.xdf.acdc.connect.jdbc.sink;

import cn.xdf.acdc.connect.core.util.EnumRecommender;
import cn.xdf.acdc.connect.core.util.EnumValidator;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import cn.xdf.acdc.connect.jdbc.util.DatabaseDialectRecommender;
import cn.xdf.acdc.connect.jdbc.util.QuoteMethod;
import cn.xdf.acdc.connect.jdbc.util.TableType;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class JdbcSinkConfig extends SinkConfig {

    public static final String CONNECTION_PREFIX = "connection.";

    public static final String CONNECTION_URL = CONNECTION_PREFIX + "url";

    public static final String CONNECTION_URL_DOC =
            "JDBC connection URL.\n"
                    + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
                    + "``jdbc:mysql://localhost/db_name``, "
                    + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
                    + "databaseName=db_name``";

    public static final String CONNECTION_URL_DISPLAY = "JDBC URL";

    public static final String CONNECTION_USER = CONNECTION_PREFIX + "user";

    public static final String CONNECTION_USER_DOC = "JDBC connection user.";

    public static final String CONNECTION_USER_DISPLAY = "JDBC User";

    public static final String CONNECTION_PASSWORD = CONNECTION_PREFIX + "password";

    public static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";

    public static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

    public static final String CONNECTION_ATTEMPTS = CONNECTION_PREFIX + "attempts";

    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

    public static final String CONNECTION_ATTEMPTS_DOC =
            "Maximum number of attempts to retrieve a valid JDBC connection. "
                    + "Must be a positive integer.";

    public static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";

    public static final String CONNECTION_BACKOFF =
            CONNECTION_PREFIX + "backoff.ms";

    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

    public static final String CONNECTION_BACKOFF_DOC = "Backoff time in milliseconds between connection attempts.";

    public static final String CONNECTION_BACKOFF_DISPLAY = "JDBC connection backoff in milliseconds";

    public static final String AUTO_CREATE = "auto.create";

    public static final String AUTO_CREATE_DEFAULT = "false";

    public static final String AUTO_CREATE_DOC =
            "Whether to automatically create the destination table based on record schema if it is "
                    + "found to be missing by issuing ``CREATE``.";

    public static final String AUTO_CREATE_DISPLAY = "Auto-Create";

    public static final String AUTO_EVOLVE = "auto.evolve";

    public static final String AUTO_EVOLVE_DEFAULT = "false";

    public static final String AUTO_EVOLVE_DOC =
            "Whether to automatically add columns in the table schema when found to be missing relative "
                    + "to the record schema by issuing ``ALTER``.";

    public static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

    public static final String INSERT_MODE = "insert.mode";

    public static final String INSERT_MODE_DEFAULT = "upsert";

    public static final String INSERT_MODE_DOC =
            "The insertion mode to use. Supported modes are:\n"
                    + "``insert``\n"
                    + "    Use standard SQL ``INSERT`` statements.\n"
                    + "``upsert``\n"
                    + "    Use the appropriate upsert semantics for the target database if it is supported by "
                    + "the connector, e.g. ``INSERT OR IGNORE``.\n"
                    + "``update``\n"
                    + "    Use the appropriate update semantics for the target database if it is supported by "
                    + "the connector, e.g. ``UPDATE``.";

    public static final String INSERT_MODE_DISPLAY = "Insert Mode";

    public static final String DIALECT_NAME_CONFIG = "dialect.name";

    public static final String DIALECT_NAME_DEFAULT = "";

    public static final String DIALECT_NAME_DISPLAY = "Database Dialect";

    public static final String DIALECT_NAME_DOC =
            "The name of the database dialect that should be used for this connector. By default this "
                    + "is empty, and the connector automatically determines the dialect based upon the "
                    + "JDBC connection URL. Use this if you want to override that behavior and use a "
                    + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
                    + "can be used.";

    public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";

    public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = QuoteMethod.ALWAYS.name();

    public static final String QUOTE_SQL_IDENTIFIERS_DOC =
            "When to quote table names, column names, and other identifiers in SQL statements. "
                    + "For backward compatibility, the default is ``always``.";

    public static final String QUOTE_SQL_IDENTIFIERS_DISPLAY = "Quote Identifiers";

    public static final String TABLE_TYPES_CONFIG = "table.types";

    public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();

    public static final String TABLE_TYPES_DISPLAY = "Table Types";

    public static final String TABLE_TYPES_DOC =
            "The comma-separated types of database tables to which the sink connector can write. "
                    + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
                    + TableType.TABLE + "`` and ``" + TableType.VIEW + "`` is allowed. Not all databases "
                    + "support writing to views, and when they do the the sink connector will fail if the "
                    + "view definition does not match the records' schemas (regardless of ``"
                    + AUTO_EVOLVE + "``).";

    public static final EnumRecommender QUOTE_METHOD_RECOMMENDER = EnumRecommender.in(QuoteMethod.values());

    public static final EnumRecommender TABLE_TYPES_RECOMMENDER = EnumRecommender.in(TableType.values());

    public static final String NUMERIC_MAPPING_CONFIG = "numeric.mapping";

    public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";

    public static final String SCHEMA_PATTERN_DEFAULT = null;

    public static final String CATALOG_PATTERN_CONFIG = "catalog.pattern";

    public static final String CATALOG_PATTERN_DEFAULT = null;

    public static final String TABLE_TYPE_CONFIG = "table.types";

    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";

    public static final String CONNECTION_GROUP = "Connection";

    // define config of this type of sink
    static {
        CONFIG_DEF.define(
                // Connection
                CONNECTION_URL,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                CONNECTION_URL_DOC,
                CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                CONNECTION_URL_DISPLAY
        ).define(
                CONNECTION_USER,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                CONNECTION_USER_DOC,
                CONNECTION_GROUP,
                2,
                ConfigDef.Width.MEDIUM,
                CONNECTION_USER_DISPLAY
        ).define(
                CONNECTION_PASSWORD,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.HIGH,
                CONNECTION_PASSWORD_DOC,
                CONNECTION_GROUP,
                3,
                ConfigDef.Width.MEDIUM,
                CONNECTION_PASSWORD_DISPLAY
        ).define(
                DIALECT_NAME_CONFIG,
                ConfigDef.Type.STRING,
                DIALECT_NAME_DEFAULT,
                DatabaseDialectRecommender.INSTANCE,
                ConfigDef.Importance.LOW,
                DIALECT_NAME_DOC,
                CONNECTION_GROUP,
                4,
                ConfigDef.Width.LONG,
                DIALECT_NAME_DISPLAY,
                DatabaseDialectRecommender.INSTANCE
        ).define(
                CONNECTION_ATTEMPTS,
                ConfigDef.Type.INT,
                CONNECTION_ATTEMPTS_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                CONNECTION_GROUP,
                5,
                ConfigDef.Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF,
                ConfigDef.Type.LONG,
                CONNECTION_BACKOFF_DEFAULT,
                ConfigDef.Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                CONNECTION_GROUP,
                6,
                ConfigDef.Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                // Writes
                INSERT_MODE,
                ConfigDef.Type.STRING,
                INSERT_MODE_DEFAULT,
                EnumValidator.in(InsertMode.values()),
                ConfigDef.Importance.HIGH,
                INSERT_MODE_DOC,
                WRITES_GROUP,
                4,
                ConfigDef.Width.MEDIUM,
                INSERT_MODE_DISPLAY
        ).define(
                TABLE_TYPES_CONFIG,
                ConfigDef.Type.LIST,
                TABLE_TYPES_DEFAULT,
                TABLE_TYPES_RECOMMENDER,
                ConfigDef.Importance.LOW,
                TABLE_TYPES_DOC,
                WRITES_GROUP,
                5,
                ConfigDef.Width.MEDIUM,
                TABLE_TYPES_DISPLAY
        ).define(
                // DDL
                AUTO_CREATE,
                ConfigDef.Type.BOOLEAN,
                AUTO_CREATE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                AUTO_CREATE_DOC, DDL_GROUP,
                1,
                ConfigDef.Width.SHORT,
                AUTO_CREATE_DISPLAY
        ).define(
                AUTO_EVOLVE,
                ConfigDef.Type.BOOLEAN,
                AUTO_EVOLVE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                AUTO_EVOLVE_DOC, DDL_GROUP,
                2,
                ConfigDef.Width.SHORT,
                AUTO_EVOLVE_DISPLAY
        ).define(
                QUOTE_SQL_IDENTIFIERS_CONFIG,
                ConfigDef.Type.STRING,
                QUOTE_SQL_IDENTIFIERS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                QUOTE_SQL_IDENTIFIERS_DOC,
                DDL_GROUP,
                3,
                ConfigDef.Width.MEDIUM,
                QUOTE_SQL_IDENTIFIERS_DISPLAY,
                QUOTE_METHOD_RECOMMENDER
        );
    }

    // 成员变量
    private final String connectionUrl;

    private final String connectionUser;

    private final String connectionPassword;

    private final int connectionAttempts;

    private final long connectionBackoffMs;

    private final boolean autoCreate;

    private final boolean autoEvolve;

    private final InsertMode insertMode;

    private final String dialectName;

    private final EnumSet<TableType> tableTypes;

    public JdbcSinkConfig(final Map<String, String> props) {
        super(props);

        connectionUrl = getString(CONNECTION_URL);
        connectionUser = getString(CONNECTION_USER);
        connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
        connectionAttempts = getInt(CONNECTION_ATTEMPTS);
        connectionBackoffMs = getLong(CONNECTION_BACKOFF);
        autoCreate = getBoolean(AUTO_CREATE);
        autoEvolve = getBoolean(AUTO_EVOLVE);
        insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
        dialectName = getString(DIALECT_NAME_CONFIG);
        tableTypes = TableType.parse(getList(TABLE_TYPES_CONFIG));
    }

    private String getPasswordValue(final String key) {
        Password password = getPassword(key);
        if (password != null) {
            return password.value();
        }
        return null;
    }

    /**
     * Get table type enum set.
     *
     * @return table type enum set
     */
    public EnumSet<TableType> tableTypes() {
        return tableTypes;
    }

    /**
     * Get table type name set.
     *
     * @return table type name set
     */
    public Set<String> tableTypeNames() {
        return tableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
    }

    public enum InsertMode {
        INSERT,
        UPSERT,
        UPDATE
    }

    public enum NumericMapping {
        NONE,
        PRECISION_ONLY,
        BEST_FIT;

        private static final Map<String, NumericMapping> REVERSE = new HashMap<>(values().length);

        static {
            for (NumericMapping val : values()) {
                REVERSE.put(val.name().toLowerCase(Locale.ROOT), val);
            }
        }

        /**
         * Get numeric mapping by prop.
         *
         * @param prop prop
         * @return numeric mapping
         */
        public static NumericMapping get(final String prop) {
            // not adding a check for null value because the recommender/validator should catch those.
            return REVERSE.get(prop.toLowerCase(Locale.ROOT));
        }
    }
}
