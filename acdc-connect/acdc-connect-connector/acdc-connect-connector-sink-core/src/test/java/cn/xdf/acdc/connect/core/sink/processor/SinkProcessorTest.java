package cn.xdf.acdc.connect.core.sink.processor;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SinkProcessorTest {
    
    private CachedSinkProcessorProvider cachedSinkProcessorProvider;
    
    private Schema keySchema = SchemaBuilder.struct().name("com.example.Person")
            .field("id", Schema.STRING_SCHEMA)
            .build();
    
    private Struct keyStruct = new Struct(keySchema)
            .put("id", "1234567");
    
    private Schema valueSchema = SchemaBuilder.struct().name("com.example.Person")
            .field("id", Schema.STRING_SCHEMA)
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("int_value", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool_value", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short_value", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte_value", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long_value", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float_value", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double_value", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("date_value", Timestamp.SCHEMA)
            .field("exclude_field", Schema.STRING_SCHEMA)
            .field("to_mapping_field", Schema.STRING_SCHEMA)
            .field("__meta_data_field", Schema.STRING_SCHEMA)
            .build();
    
    private Date now = new Date();
    
    private Struct valueStruct = new Struct(valueSchema)
            .put("id", "1234567")
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool_value", true)
            .put("short_value", (short) 10)
            .put("byte_value", (byte) 10)
            .put("long_value", 10L)
            .put("float_value", (float) 10.0)
            .put("double_value", (double) 10.0)
            .put("int_value", 10)
            .put("date_value", now)
            .put("exclude_field", "exclude_value")
            .put("to_mapping_field", "to_mapping_value")
            .put("__meta_data_field", "__meta_data_value");
    
    private SinkRecord sinkRecord = new SinkRecord("test_topic", 1, keySchema, keyStruct, valueSchema, valueStruct, 42);
    
    private Schema deletedSchema = SchemaBuilder.struct().name("com.example.Person")
            .field("id", Schema.STRING_SCHEMA)
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("int_value", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool_value", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short_value", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte_value", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long_value", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float_value", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double_value", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("date_value", Timestamp.SCHEMA)
            .field("exclude_field", Schema.STRING_SCHEMA)
            .field("to_mapping_field", Schema.STRING_SCHEMA)
            .field("__deleted", Schema.STRING_SCHEMA)
            .build();
    
    private Struct deletedStruct = new Struct(deletedSchema)
            .put("id", "1234567")
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool_value", true)
            .put("short_value", (short) 10)
            .put("byte_value", (byte) 10)
            .put("long_value", 10L)
            .put("float_value", (float) 10.0)
            .put("double_value", (double) 10.0)
            .put("int_value", 10)
            .put("date_value", now)
            .put("exclude_field", "excloud_value")
            .put("to_mapping_field", "to_mapping_value")
            .put("__deleted", "true");
    
    private SinkRecord deletedSinkRecord = new SinkRecord("test_topic", 1, keySchema, keyStruct, deletedSchema, deletedStruct, 42);
    
    @Before
    public void setUp() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("name", "unit_test_connector");
        configs.put("batch.size", "100");
        configs.put("delete.enabled", "true");
        configs.put("max.retries", "3");
        configs.put("retry.backoff.ms", "2000");
        configs.put("pk.mode", "RECORD_KEY");
        configs.put("pk.fields", "pk");
        configs.put("destinations", "logical_delete_table, physical_delete_table, conflict_table");
        
        configs.put("destinations.logical_delete_table.fields.whitelist", "id,firstName,lastName,to_mapping_field");
        configs.put("destinations.logical_delete_table.fields.mapping", "id:new_id,firstName:new_firstName,lastName:new_lastName,to_mapping_field:new_field");
        configs.put("destinations.logical_delete_table.fields.add", "added_field_1:value_1,added_field_2:value_2");
        configs.put("destinations.logical_delete_table.row.filter", "");
        configs.put("destinations.logical_delete_table.delete.mode", "LOGICAL");
        configs.put("destinations.logical_delete_table.delete.logical.field.name", "is_delete");
        configs.put("destinations.logical_delete_table.delete.logical.field.value", "1");
        
        configs.put("destinations.physical_delete_table.fields.whitelist",
                "id,firstName,lastName,bool_value,short_value,byte_value,long_value,float_value,double_value,int_value,date_value,to_mapping_field");
        configs.put("destinations.physical_delete_table.fields.mapping", "to_mapping_field:new_field");
        configs.put("destinations.physical_delete_table.fields.add", "added_field_1:value_1,added_field_2:value_2");
        configs.put("destinations.physical_delete_table.row.filter", "");
        configs.put("destinations.physical_delete_table.delete.mode", "PHYSICAL");
        
        configs.put("destinations.conflict_table.fields.whitelist", "firstName,lastName");
        configs.put("destinations.conflict_table.fields.mapping", "firstName:lastName,lastName:lastName_mapped");
        configs.put("destinations.conflict_table.row.filter", "");
        configs.put("destinations.physical_delete_table.delete.mode", "PHYSICAL");
        
        SinkConfig sinkConfig = new SinkConfig(configs);
        cachedSinkProcessorProvider = new CachedSinkProcessorProvider(sinkConfig);
    }
    
    @Test
    public void shouldLogicalDelete() {
        SinkProcessor sinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("logical_delete_table");
        SinkRecord newSinkRecord = sinkSingleRecordProcessor.process(deletedSinkRecord);
        
        Struct recordValue = (Struct) newSinkRecord.value();
        Assert.assertEquals(recordValue.get("is_delete"), "1");
    }
    
    @Test
    public void shouldPhysicalDelete() {
        SinkProcessor sinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("physical_delete_table");
        SinkRecord newSinkRecord = sinkSingleRecordProcessor.process(deletedSinkRecord);
        
        Assert.assertNull(newSinkRecord.value());
        Assert.assertNull(newSinkRecord.valueSchema());
    }
    
    @Test
    public void shouldExecuteFieldsMappingAndAdd() {
        SinkProcessor sinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("logical_delete_table");
        SinkRecord newSinkRecord = sinkSingleRecordProcessor.process(sinkRecord);
        
        // add two field
        Assert.assertEquals(7, newSinkRecord.valueSchema().fields().size());
        
        Struct newRecordValue = (Struct) newSinkRecord.value();
        Struct newRecordkey = (Struct) newSinkRecord.key();
        Struct recordValue = (Struct) sinkRecord.value();
        Struct recordKey = (Struct) sinkRecord.key();
        
        Assert.assertEquals(recordValue.get("to_mapping_field"), newRecordValue.get("new_field"));
        Assert.assertEquals(recordValue.get("id"), newRecordValue.get("new_id"));
        Assert.assertEquals(recordValue.get("firstName"), newRecordValue.get("new_firstName"));
        Assert.assertEquals(recordValue.get("lastName"), newRecordValue.get("new_lastName"));
        
        Assert.assertEquals("value_1", newRecordValue.get("added_field_1"));
        Assert.assertEquals("value_2", newRecordValue.get("added_field_2"));
        
        Assert.assertEquals(recordKey.get("id"), newRecordkey.get("new_id"));
    }
    
    @Test(expected = DataException.class)
    public void shouldThrowDataException() {
        SinkProcessor sinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("logical_delete_table");
        SinkRecord newSinkRecord = sinkSingleRecordProcessor.process(sinkRecord);
        
        Struct recordValue = (Struct) newSinkRecord.value();
        recordValue.get("exclude_field");
    }
    
    @Test
    public void shouldRetainMetaData() {
        SinkProcessor sinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("logical_delete_table");
        SinkRecord newSinkRecord = sinkSingleRecordProcessor.process(sinkRecord);
        
        Struct recordValue = (Struct) newSinkRecord.value();
        Assert.assertNotNull(recordValue.get("__meta_data_field"));
    }
    
    @Test
    public void shouldDoCorrectProcessWhenFieldNameConflicted() {
        SinkProcessor sinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("conflict_table");
        SinkRecord newSinkRecord = sinkSingleRecordProcessor.process(sinkRecord);
        
        Struct newRecordValue = (Struct) newSinkRecord.value();
        
        Assert.assertNotNull(newRecordValue.get("lastName"));
        Assert.assertNotNull(newRecordValue.get("lastName_mapped"));
        
        Struct recordValue = (Struct) sinkRecord.value();
        Assert.assertEquals(recordValue.get("firstName"), newRecordValue.get("lastName"));
        Assert.assertEquals(recordValue.get("lastName"), newRecordValue.get("lastName_mapped"));
    }
    
}
