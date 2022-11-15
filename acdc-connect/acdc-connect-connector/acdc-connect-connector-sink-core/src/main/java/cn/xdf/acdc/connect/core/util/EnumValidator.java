package cn.xdf.acdc.connect.core.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class EnumValidator implements ConfigDef.Validator {

    private final List<String> canonicalValues;

    private final Set<String> validValues;

    private EnumValidator(final List<String> canonicalValues, final Set<String> validValues) {
        this.canonicalValues = canonicalValues;
        this.validValues = validValues;
    }

    /**
     * Factory method for {@link EnumValidator}.
     *
     * @param enumerators enumerators included in this validator
     * @param <E>         type of enumerators
     * @return instance of EnumValidator
     */
    public static <E> EnumValidator in(final E[] enumerators) {
        final List<String> canonicalValues = new ArrayList<>(enumerators.length);
        final Set<String> validValues = new HashSet<>(enumerators.length * 2);
        for (E e : enumerators) {
            canonicalValues.add(e.toString().toLowerCase());
            validValues.add(e.toString().toUpperCase());
            validValues.add(e.toString().toLowerCase());
        }
        return new EnumValidator(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(final String key, final Object value) {
        if (!validValues.contains(value)) {
            throw new ConfigException(key, value, "Invalid enumerator");
        }
    }

    @Override
    public String toString() {
        return canonicalValues.toString();
    }
}
