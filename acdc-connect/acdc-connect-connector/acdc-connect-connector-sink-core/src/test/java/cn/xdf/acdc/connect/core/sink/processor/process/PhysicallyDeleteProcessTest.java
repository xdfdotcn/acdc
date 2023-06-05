package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PhysicallyDeleteProcessTest {
    
    private PhysicallyDeletionProcess physicallyDeleteSingleRecordProcess;
    
    @Before
    public void setUp() throws Exception {
        physicallyDeleteSingleRecordProcess = new PhysicallyDeletionProcess();
    }
    
    @Test
    public void shouldDelete() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        fieldAndValues.put("__deleted", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "__delete", "true"));
        boolean processReturn = physicallyDeleteSingleRecordProcess.execute(fieldAndValues);
        
        Assert.assertFalse(processReturn);
        Assert.assertTrue(fieldAndValues.isEmpty());
    }
    
    @Test
    public void shouldNotDeleteWhenDeleteSignalValueIsFalse() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        fieldAndValues.put("__deleted", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "__delete", "false"));
        boolean processReturn = physicallyDeleteSingleRecordProcess.execute(fieldAndValues);
        
        Assert.assertTrue(processReturn);
        Assert.assertEquals(fieldAndValues.size(), 4);
    }
    
    @Test
    public void shouldNotDeleteWhenDeleteSignalFieldNotExisted() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        boolean processReturn = physicallyDeleteSingleRecordProcess.execute(fieldAndValues);
        
        Assert.assertTrue(processReturn);
        Assert.assertEquals(fieldAndValues.size(), 3);
    }
}
