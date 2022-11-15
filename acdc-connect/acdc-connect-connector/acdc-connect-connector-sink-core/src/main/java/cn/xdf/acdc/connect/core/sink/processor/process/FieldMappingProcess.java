package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class FieldMappingProcess implements Process {

    // origin name mapping to new name
    private final Map<String, String> fieldMappings;

    public FieldMappingProcess(final Map<String, String> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    @Override
    public boolean execute(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        if (!fieldMappings.isEmpty()) {
            Map<String, TemporaryFieldAndValue> mappedFieldAndValues = new HashMap<>();
            for (Entry<String, String> each : fieldMappings.entrySet()) {
                if (fieldAndValues.containsKey(each.getKey())) {
                    TemporaryFieldAndValue fieldAndValue = fieldAndValues.remove(each.getKey());
                    String newName = each.getValue();
                    fieldAndValue.setName(newName);
                    mappedFieldAndValues.put(newName, fieldAndValue);
                }
            }
            fieldAndValues.putAll(mappedFieldAndValues);
        }

        return true;
    }

}
