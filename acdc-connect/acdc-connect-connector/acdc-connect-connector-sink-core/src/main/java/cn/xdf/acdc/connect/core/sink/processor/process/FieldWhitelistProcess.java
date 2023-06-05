package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;

import java.util.Map;
import java.util.Set;

public class FieldWhitelistProcess implements Process {
    
    private final Set<String> fieldsWhitelist;
    
    public FieldWhitelistProcess(final Set<String> fieldsWhitelist) {
        this.fieldsWhitelist = fieldsWhitelist;
    }
    
    @Override
    public boolean execute(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        // delete the field if it's not a metadata and not in fields white list and fields white list is set.
        if (!fieldsWhitelist.isEmpty()) {
            fieldAndValues.entrySet().removeIf(each ->
                    !each.getValue().isMetaDataField() && !fieldsWhitelist.contains(each.getKey())
            );
        }
        return true;
    }
}
