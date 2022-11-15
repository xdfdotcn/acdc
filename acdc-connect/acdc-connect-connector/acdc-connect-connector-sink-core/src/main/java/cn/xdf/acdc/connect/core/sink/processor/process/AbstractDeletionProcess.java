package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;

import java.util.Map;
import java.util.Objects;

public abstract class AbstractDeletionProcess implements Process {

    public static final String DELETE_SIGNAL_FIELD_NAME = "__deleted";

    public static final String DELETE_SIGNAL_FIELD_VALUE = "true";

    @Override
    public boolean execute(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        TemporaryFieldAndValue deleteSignalFieldAndName = fieldAndValues.get(DELETE_SIGNAL_FIELD_NAME);
        if (Objects.nonNull(deleteSignalFieldAndName)) {
            if (DELETE_SIGNAL_FIELD_VALUE.equalsIgnoreCase(deleteSignalFieldAndName.getValue().toString())) {
                return doDelete(fieldAndValues);
            } else {
                return keepExist(fieldAndValues);
            }
        }
        return true;
    }

    abstract boolean doDelete(Map<String, TemporaryFieldAndValue> fieldAndValues);

    abstract boolean keepExist(Map<String, TemporaryFieldAndValue> fieldAndValues);

}
