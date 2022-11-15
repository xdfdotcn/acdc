package cn.xdf.acdc.connect.core.sink.processor;

import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CachedSinkProcessorProviderTest {

    private CachedSinkProcessorProvider cachedSinkProcessorProvider;

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
        cachedSinkProcessorProvider = new CachedSinkProcessorProvider(sinkConfig);
    }

    @Test
    public void shouldGetProcessor() {
        SinkProcessor table1SinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("table_1");
        SinkProcessor table2SinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("table_2");

        assertTrue(Objects.nonNull(table1SinkSingleRecordProcessor));
        assertTrue(Objects.nonNull(table2SinkSingleRecordProcessor));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotGetProcessor() {
        cachedSinkProcessorProvider.getProcessor("table_3");
    }

    @Test
    public void shouldGetCachedProcessor() {
        SinkProcessor table1SinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("table_1");
        SinkProcessor table1CachedSinkSingleRecordProcessor = cachedSinkProcessorProvider.getProcessor("table_1");

        assertEquals(table1CachedSinkSingleRecordProcessor, table1SinkSingleRecordProcessor);
    }

}
