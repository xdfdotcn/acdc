package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FieldAdditionProcessTest {

    private FieldAdditionProcess fieldAddSingleRecordProcess;

    @Before
    public void setUp() throws Exception {
        Map<String, String> fieldsToBeAdded = new HashMap<>();
        fieldsToBeAdded.put("string_field_1", "new_value_1");
        fieldsToBeAdded.put("string_field_2", "new_value_2");
        fieldsToBeAdded.put("string_field_3", "new_value_3");
        //
        fieldsToBeAdded.put("acdc_update_time", "${datetime}");
        fieldAddSingleRecordProcess = new FieldAdditionProcess(fieldsToBeAdded, TimeZone.getTimeZone("UTC"));
    }

    @Test
    public void addStringFieldAndPlaceholderField() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAddSingleRecordProcess.execute(fieldAndValues);

        assertEquals(fieldAndValues.size(), 4);
        assertFalse(fieldAndValues.get("acdc_update_time").getValue().equals("${datetime}"));
    }

    @Test
    public void shouldCoverValueWhenFieldNameExisted() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("string_field_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "string_field_1", "old_value"));
        fieldAddSingleRecordProcess.execute(fieldAndValues);

        assertEquals(fieldAndValues.size(), 4);
        assertEquals(fieldAndValues.get("string_field_1").getValue(), "new_value_1");
    }

    @Test
    public void testShouldReturnZonedTimestampWhenAddStringFieldAndPlaceholderField() {
        Map<String, String> fieldsToBeAdded = new HashMap<>();
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldsToBeAdded.put("acdc_update_time", "${datetime}");

        fieldAddSingleRecordProcess = new FieldAdditionProcess(fieldsToBeAdded, TimeZone.getTimeZone("Asia/Shanghai"));
        fieldAddSingleRecordProcess.execute(fieldAndValues);

        String zonedTimestamp = String.valueOf(fieldAndValues.get("acdc_update_time").getValue());
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(zonedTimestamp, ZonedTimestamp.FORMATTER);

        assertEquals("+08:00", zonedDateTime.getZone().getId());
        assertEquals(ZonedTimestamp.LOGICAL_NAME, fieldAndValues.get("acdc_update_time").getSchema().name());
    }
}
