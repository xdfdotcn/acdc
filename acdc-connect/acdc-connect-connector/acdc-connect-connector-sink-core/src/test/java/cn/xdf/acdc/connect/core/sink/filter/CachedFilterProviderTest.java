package cn.xdf.acdc.connect.core.sink.filter;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CachedFilterProviderTest {
    
    private CachedFilterProvider cachedFilterProvider;
    
    @Before
    public void setUp() {
        Map<String, String> configs = new HashMap<>();
        configs.put("name", "unit_test_connector");
        configs.put("batch.size", "3000");
        configs.put("delete.enabled", "true");
        configs.put("max.retries", "3");
        configs.put("retry.backoff.ms", "2000");
        configs.put("destinations", "table_1,table_2");
        
        configs.put("destinations.table_1.fields.whitelist", "pk,field_1,field_2,field_3");
        configs.put("destinations.table_1.fields.mapping", "field_1:new_field_1");
        configs.put("destinations.table_1.fields.add", "added_field_1:value_1,added_field_2:value_2");
        configs.put("destinations.table_1.row.filter", "");
        configs.put("destinations.table_1.delete.mode", "LOGICAL");
        configs.put("destinations.table_1.delete.logical.field.name", "is_delete");
        configs.put("destinations.table_1.delete.logical.field.value", "1");
        
        configs.put("destinations.table_2.fields.whitelist", "pk,field_1,field_2,field_3");
        configs.put("destinations.table_2.fields.mapping", "field_1:new_field_1");
        configs.put("destinations.table_2.fields.add", "added_field_1:value_1,added_field_2:value_2");
        configs.put("destinations.table_2.row.filter", "");
        configs.put("destinations.table_2.delete.mode", "LOGICAL");
        configs.put("destinations.table_2.delete.logical.field.name", "is_delete");
        configs.put("destinations.table_2.delete.logical.field.value", "1");
        
        SinkConfig sinkConfig = new SinkConfig(configs);
        cachedFilterProvider = new CachedFilterProvider(sinkConfig);
    }
    
    @Test
    public void shouldGetFilter() {
        Filter table1Filter = cachedFilterProvider.getFilter("table_1");
        Filter table2Filter = cachedFilterProvider.getFilter("table_2");
        
        Assert.assertTrue(Objects.nonNull(table1Filter));
        Assert.assertTrue(Objects.nonNull(table2Filter));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotGetFilter() {
        cachedFilterProvider.getFilter("table_3");
    }
    
    @Test
    public void shouldGetCachedFilter() {
        Filter table1Filter = cachedFilterProvider.getFilter("table_1");
        Filter table2Filter = cachedFilterProvider.getFilter("table_1");
        
        Assert.assertEquals(table1Filter, table2Filter);
    }
    
}
