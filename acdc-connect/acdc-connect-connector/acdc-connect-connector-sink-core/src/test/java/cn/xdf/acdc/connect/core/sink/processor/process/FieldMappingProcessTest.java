package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FieldMappingProcessTest {

    private FieldMappingProcess fieldMappingSingleRecordProcess;

    @Before
    public void setUp() throws Exception {
        Map<String, String> fieldMappings = new HashMap<>();
        fieldMappings.put("filed_name_1", "new_filed_name_1");
        fieldMappings.put("filed_name_2", "new_filed_name_2");
        fieldMappings.put("filed_name_3", "new_filed_name_3");
        fieldMappingSingleRecordProcess = new FieldMappingProcess(fieldMappings);
    }

    @Test
    public void shouldMappingAllFields() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("filed_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "filed_name_1", "value_1"));
        fieldAndValues.put("filed_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "filed_name_2", "value_2"));
        fieldAndValues.put("filed_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "filed_name_3", "value_3"));
        fieldMappingSingleRecordProcess.execute(fieldAndValues);

        assertEquals(fieldAndValues.size(), 3);
        assertTrue(fieldAndValues.get("new_filed_name_1").getValue().equals("value_1"));
        assertTrue(fieldAndValues.get("new_filed_name_2").getValue().equals("value_2"));
        assertTrue(fieldAndValues.get("new_filed_name_3").getValue().equals("value_3"));
    }

    @Test
    public void shouldNotMappingAllFields() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("filed_name_4", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "filed_name_4", "value_1"));
        fieldAndValues.put("filed_name_5", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "filed_name_5", "value_2"));
        fieldAndValues.put("filed_name_6", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "filed_name_6", "value_3"));
        fieldMappingSingleRecordProcess.execute(fieldAndValues);

        assertEquals(fieldAndValues.size(), 3);
        assertTrue(fieldAndValues.get("filed_name_4").getValue().equals("value_1"));
        assertTrue(fieldAndValues.get("filed_name_5").getValue().equals("value_2"));
        assertTrue(fieldAndValues.get("filed_name_6").getValue().equals("value_3"));
    }

}
