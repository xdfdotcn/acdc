package cn.xdf.acdc.connect.core.util.config;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SinkConfigTest {
    
    private Map<String, String> props;
    
    @Before
    public void setUp() {
        props = new HashMap<>();
        props.put(SinkConfig.DESTINATIONS, " table_1 , table_2 ");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST, "b1,b2,b3");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_2" + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST, "b21,b22,b23");
    }
    
    @Test
    public void createConfig() {
        new SinkConfig(props);
    }
    
    @Test
    public void testTablesParseToArray() {
        String[] expectArray = {"table_1", "table_2"};
        SinkConfig sinkConfig = new SinkConfig(props);
        Assert.assertArrayEquals(expectArray, sinkConfig.getDestinations().toArray());
    }
    
    @Test
    public void testWhiteListSetToEmpty() {
        props.remove(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_FIELD_WHITELIST);
        new SinkConfig(props);
    }
    
    @Test
    public void testTableConfigsDefaultValues() {
        SinkConfig sinkConfig = new SinkConfig(props);
        // default value check
        Assert.assertTrue(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsMapping().isEmpty());
        Assert.assertTrue(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsToAdd().isEmpty());
        Assert.assertTrue(sinkConfig.getDestinationConfigMapping().get("table_1").getRowFilterExpress().isEmpty());
        Assert.assertEquals(DeleteMode.PHYSICAL, sinkConfig.getDestinationConfigMapping().get("table_1").getDeleteMode());
        Assert.assertEquals(SinkConfig.LOGICAL_DELETE_FIELD_NAME_DEFAULT, sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldName());
        Assert.assertEquals(SinkConfig.LOGICAL_DELETE_FIELD_VALUE_DELETED_DEFAULT, sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldValueDeleted());
        Assert.assertEquals(SinkConfig.LOGICAL_DELETE_FIELD_VALUE_NORMAL_DEFAULT, sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldValueNormal());
    }
    
    @Test
    public void testParseStringValuesToStruct() {
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_FIELD_MAPPING, "b1: a1,b2 :a2");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_FIELD_ADD, "add_field_1:${Date},add_field_2:str_value");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_ROW_FILTER, "b1=10");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_DELETE_MODE, "LOGICAL");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_NAME, "is_delete");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_VALUE_DELETED, "2");
        props.put(SinkConfig.DESTINATIONS_CONFIG_PREFIX + "table_1" + SinkConfig.DESTINATIONS_CONFIG_DELETE_LOGICAL_FIELD_VALUE_NORMAL, "-1");
        SinkConfig sinkConfig = new SinkConfig(props);
        
        Map<String, String> fieldsMapping = new HashMap<>();
        fieldsMapping.put("b1", "a1");
        fieldsMapping.put("b2", "a2");
        
        Assert.assertEquals(fieldsMapping, sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsMapping());
        Assert.assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsToAdd().get("add_field_2"), "str_value");
        Assert.assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsToAdd().get("add_field_1"), "${Date}");
        Assert.assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getRowFilterExpress(), "b1=10");
        Assert.assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getDeleteMode(), DeleteMode.LOGICAL);
        Assert.assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldName(), "is_delete");
        Assert.assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldValueDeleted(), "2");
    }
    
    @Test
    public void testParseWhiteList() {
        SinkConfig sinkConfig = new SinkConfig(props);
        Assert.assertEquals(Sets.newHashSet("b1", "b2", "b3"), sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsWhitelist());
        Assert.assertEquals(Sets.newHashSet("b21", "b22", "b23"), sinkConfig.getDestinationConfigMapping().get("table_2").getFieldsWhitelist());
    }
    
}
