package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;

import java.util.Map;

public class PhysicallyDeletionProcess extends AbstractDeletionProcess {

    @Override
    boolean doDelete(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        fieldAndValues.clear();
        return false;
    }

    @Override
    boolean keepExist(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        return true;
    }

}
