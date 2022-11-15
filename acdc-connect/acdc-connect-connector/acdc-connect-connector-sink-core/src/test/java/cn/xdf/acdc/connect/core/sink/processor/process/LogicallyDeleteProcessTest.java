package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogicallyDeleteProcessTest {

    private LogicallyDeletionProcess logicallyDeleteSingleRecordProcess;

    @Before
    public void setUp() throws Exception {
        logicallyDeleteSingleRecordProcess = new LogicallyDeletionProcess("is_delete", "1", "-1");
    }

    @Test
    public void shouldDelete() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        fieldAndValues.put("__deleted", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "__delete", "true"));
        boolean processReturn = logicallyDeleteSingleRecordProcess.execute(fieldAndValues);

        assertTrue(processReturn);
        assertEquals(fieldAndValues.size(), 5);
        assertEquals("1", fieldAndValues.get("is_delete").getValue());
    }

    @Test
    public void shouldNotDeleteWhenDeleteSignalValueIsFalse() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        fieldAndValues.put("__deleted", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "__delete", "false"));
        boolean processReturn = logicallyDeleteSingleRecordProcess.execute(fieldAndValues);

        assertTrue(processReturn);
        assertEquals(fieldAndValues.size(), 5);
        assertEquals("-1", fieldAndValues.get("is_delete").getValue());
    }

    @Test
    public void shouldNotDeleteWhenDeleteSignalFieldNotExisted() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        boolean processReturn = logicallyDeleteSingleRecordProcess.execute(fieldAndValues);

        assertTrue(processReturn);
        assertEquals(fieldAndValues.size(), 3);
        assertFalse(fieldAndValues.containsKey("is_delete"));
    }

}
