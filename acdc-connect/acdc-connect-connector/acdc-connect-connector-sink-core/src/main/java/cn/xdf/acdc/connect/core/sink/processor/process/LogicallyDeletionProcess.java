package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class LogicallyDeletionProcess extends AbstractDeletionProcess {

    private String logicalDeleteFieldName;

    private String logicalDeleteFieldValueDeleted;

    private String logicalDeleteFieldValueNormal;

    public LogicallyDeletionProcess(final String logicalDeleteFieldName, final String logicalDeleteFieldValueDeleted, final String logicalDeleteFieldValueNormal) {
        this.logicalDeleteFieldName = logicalDeleteFieldName;
        this.logicalDeleteFieldValueDeleted = logicalDeleteFieldValueDeleted;
        this.logicalDeleteFieldValueNormal = logicalDeleteFieldValueNormal;
    }

    @Override
    public boolean doDelete(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        fieldAndValues.put(logicalDeleteFieldName, new TemporaryFieldAndValue(Schema.STRING_SCHEMA, logicalDeleteFieldName, logicalDeleteFieldValueDeleted));
        return true;
    }

    @Override
    boolean keepExist(final Map<String, TemporaryFieldAndValue> fieldAndValues) {
        fieldAndValues.put(logicalDeleteFieldName, new TemporaryFieldAndValue(Schema.STRING_SCHEMA, logicalDeleteFieldName, logicalDeleteFieldValueNormal));
        return true;
    }

}
