package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.core.util.DateUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.function.Function;

/**
 * Add the extra field to the record.
 * New field value will cover the origin one if they have same name.
 */
public class FieldAdditionProcess implements Process {

    private static final String DATETIME_PLACEHOLDER = "${datetime}";

    private Map<String, Function<String, TemporaryFieldAndValue>> placeholderParsers = new HashMap<>();

    private Map<String, String> fieldsToBeAdded;

    private TimeZone timeZone;

    public FieldAdditionProcess(final Map<String, String> fieldsToBeAdded, final TimeZone timeZone) {
        this.fieldsToBeAdded = fieldsToBeAdded;
        this.timeZone = timeZone;
        initPlaceholderParsers();
    }

    private void initPlaceholderParsers() {
        placeholderParsers.put(DATETIME_PLACEHOLDER, this::handleDatetimePlaceholder);
    }

    @Override
    public boolean execute(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        for (Entry<String, String> each : fieldsToBeAdded.entrySet()) {
            fieldAndValues.put(each.getKey(), createTemporaryFieldAndValue(each.getKey(), each.getValue()));
        }
        return true;
    }

    private TemporaryFieldAndValue createTemporaryFieldAndValue(final String configName, final String configValue) {
        if (placeholderParsers.containsKey(configValue)) {
            return placeholderParsers.get(configValue).apply(configName);
        } else {
            return new TemporaryFieldAndValue(Schema.STRING_SCHEMA, configName, configValue);
        }
    }

    private TemporaryFieldAndValue handleDatetimePlaceholder(final String configName) {
        Schema schema = SchemaBuilder.string().name(ZonedTimestamp.LOGICAL_NAME).build();
        String zonedTimestamp = DateUtils.formatWithZone(Calendar.getInstance().getTime(), timeZone.toZoneId());
        return new TemporaryFieldAndValue(schema, configName, zonedTimestamp);
    }
}
