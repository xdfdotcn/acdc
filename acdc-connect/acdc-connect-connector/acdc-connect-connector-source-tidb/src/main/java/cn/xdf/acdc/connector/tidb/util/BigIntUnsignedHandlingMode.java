package cn.xdf.acdc.connector.tidb.util;

import io.debezium.config.EnumeratedValue;
import io.debezium.jdbc.JdbcValueConverters;

import java.math.BigDecimal;

/**
 * The set of predefined BigIntUnsignedHandlingMode options or aliases.
 */
public enum BigIntUnsignedHandlingMode implements EnumeratedValue {
    /**
     * Represent {@code BIGINT UNSIGNED} values as precise {@link BigDecimal} values, which are
     * represented in change events in a binary form. This is precise but difficult to use.
     */
    PRECISE("precise"),

    /**
     * Represent {@code BIGINT UNSIGNED} values as precise {@code long} values. This may be less precise
     * but is far easier to use.
     */
    LONG("long");

    private final String value;

    BigIntUnsignedHandlingMode(final String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    /**
     * Get bigint unsigned mode.
     *
     * @return bigint unsigned mode
     */
    public JdbcValueConverters.BigIntUnsignedMode asBigIntUnsignedMode() {
        switch (this) {
            case LONG:
                return JdbcValueConverters.BigIntUnsignedMode.LONG;
            case PRECISE:
            default:
                return JdbcValueConverters.BigIntUnsignedMode.PRECISE;
        }
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @return the matching option, or null if no match is found
     */
    public static BigIntUnsignedHandlingMode parse(final String value) {
        if (value == null) {
            return null;
        }
        for (BigIntUnsignedHandlingMode option : BigIntUnsignedHandlingMode.values()) {
            if (option.getValue().equalsIgnoreCase(value.trim())) {
                return option;
            }
        }
        return null;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value        the configuration property value; may not be null
     * @param defaultValue the default value; may be null
     * @return the matching option, or null if no match is found and the non-null default is invalid
     */
    public static BigIntUnsignedHandlingMode parse(final String value, final String defaultValue) {
        BigIntUnsignedHandlingMode mode = parse(value);
        if (mode == null && defaultValue != null) {
            mode = parse(defaultValue);
        }
        return mode;
    }
}
