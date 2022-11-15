package cn.xdf.acdc.connect.core.util.config;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        assertArrayEquals(expectArray, sinkConfig.getDestinations().toArray());
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
        assertTrue(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsMapping().isEmpty());
        assertTrue(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsToAdd().isEmpty());
        assertTrue(sinkConfig.getDestinationConfigMapping().get("table_1").getRowFilterExpress().isEmpty());
        assertEquals(DeleteMode.PHYSICAL, sinkConfig.getDestinationConfigMapping().get("table_1").getDeleteMode());
        assertEquals(SinkConfig.LOGICAL_DELETE_FIELD_NAME_DEFAULT, sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldName());
        assertEquals(SinkConfig.LOGICAL_DELETE_FIELD_VALUE_DELETED_DEFAULT, sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldValueDeleted());
        assertEquals(SinkConfig.LOGICAL_DELETE_FIELD_VALUE_NORMAL_DEFAULT, sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldValueNormal());
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
        Map<String, String> fieldsMapping = new HashMap<String, String>() {
            {
                put("b1", "a1");
                put("b2", "a2");
            }
        };
        assertEquals(fieldsMapping, sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsMapping());
        assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsToAdd().get("add_field_2"), "str_value");
        assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsToAdd().get("add_field_1"), "${Date}");
        assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getRowFilterExpress(), "b1=10");
        assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getDeleteMode(), DeleteMode.LOGICAL);
        assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldName(), "is_delete");
        assertEquals(sinkConfig.getDestinationConfigMapping().get("table_1").getLogicalDeleteFieldValueDeleted(), "2");
    }

    @Test
    public void testParseWhiteList() {
        SinkConfig sinkConfig = new SinkConfig(props);
        assertEquals(Sets.newHashSet("b1", "b2", "b3"), sinkConfig.getDestinationConfigMapping().get("table_1").getFieldsWhitelist());
        assertEquals(Sets.newHashSet("b21", "b22", "b23"), sinkConfig.getDestinationConfigMapping().get("table_2").getFieldsWhitelist());
    }

}
