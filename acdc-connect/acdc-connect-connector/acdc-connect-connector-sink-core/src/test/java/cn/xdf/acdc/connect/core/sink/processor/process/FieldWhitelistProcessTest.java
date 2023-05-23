package cn.xdf.acdc.connect.core.sink.processor.process;

import cn.xdf.acdc.connect.core.sink.data.TemporaryFieldAndValue;
import com.google.common.collect.Sets;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FieldWhitelistProcessTest {
    
    private FieldWhitelistProcess fieldWhitelistSingleRecordProcess;
    
    @Before
    public void setUp() throws Exception {
        Set<String> fieldWhitelist = Sets.newHashSet("field_name_1", "field_name_2", "field_name_3");
        fieldWhitelistSingleRecordProcess = new FieldWhitelistProcess(fieldWhitelist);
    }
    
    @Test
    public void shouldRemainAllFields() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_3", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_3", "value_3"));
        fieldWhitelistSingleRecordProcess.execute(fieldAndValues);
        
        Assert.assertEquals(fieldAndValues.size(), 3);
        Assert.assertTrue(fieldAndValues.containsKey("field_name_1"));
        Assert.assertTrue(fieldAndValues.containsKey("field_name_2"));
        Assert.assertTrue(fieldAndValues.containsKey("field_name_3"));
    }
    
    @Test
    public void shouldRemainSomeFields() {
        Map<String, TemporaryFieldAndValue> fieldAndValues = new HashMap<>();
        fieldAndValues.put("field_name_1", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_1", "value_1"));
        fieldAndValues.put("field_name_2", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_2", "value_2"));
        fieldAndValues.put("field_name_4", new TemporaryFieldAndValue(Schema.STRING_SCHEMA, "field_name_4", "value_4"));
        fieldWhitelistSingleRecordProcess.execute(fieldAndValues);
        
        Assert.assertEquals(fieldAndValues.size(), 2);
        Assert.assertTrue(fieldAndValues.containsKey("field_name_1"));
        Assert.assertTrue(fieldAndValues.containsKey("field_name_2"));
        Assert.assertFalse(fieldAndValues.containsKey("field_name_4"));
    }
    
}
