package cn.xdf.acdc.devops.biz.connect;

import cn.xdf.acdc.devops.biz.connect.response.ConnectorStatusResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class ConnectClusterRestTest {

    private static final String MOCK_URL = "http://mock-url:8083";

    private static final String CONNECTORS_MOCK_URL = MOCK_URL + "/connectors";

    private static final String CONNECTOR_NAME = "connector-1";

    private static final String CONNECTOR_NAME_NEW = "connector-2";

    @Mock
    private RestTemplate restTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private ConnectClusterRest connectClusterRest;

    @Before
    public void setup() {
        connectClusterRest = new ConnectClusterRest(restTemplate);
    }

    @Test
    public void testGetAllConnectByClusterUrlShouldGenerateUrlAsExpect() {
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(restTemplate.getForObject(argumentCaptor.capture(), ArgumentMatchers.eq(List.class))).thenReturn(Lists.newArrayList(CONNECTOR_NAME));
        List<String> connectorList = connectClusterRest.getAllConnectorByClusterUrl(MOCK_URL);
        Assert.assertEquals(CONNECTORS_MOCK_URL, argumentCaptor.getValue());
        Assert.assertEquals(Lists.newArrayList(CONNECTOR_NAME), connectorList);
    }

    @Test
    public void testCreateConnectorAsExpect() throws JsonProcessingException {
        Map<String, String> config = fakeConnectorConfig();
        connectClusterRest.createConnector(MOCK_URL, CONNECTOR_NAME, config);
        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HttpEntity> requestCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        Mockito.verify(restTemplate).postForObject(urlCaptor.capture(), requestCaptor.capture(), ArgumentMatchers.eq(String.class));
        Assert.assertEquals(CONNECTORS_MOCK_URL, urlCaptor.getValue());
        HttpEntity httpEntity = requestCaptor.getValue();
        Assert.assertEquals(MediaType.APPLICATION_JSON, httpEntity.getHeaders().getContentType());
        JsonNode jsonNode = objectMapper.readTree((String) httpEntity.getBody());
        Assert.assertEquals(CONNECTOR_NAME, jsonNode.get("name").textValue());
        JsonNode actualConfigJsonNode = jsonNode.get("config");
        Map<String, String> actualConfigMap = new HashMap<>();
        actualConfigJsonNode.fields().forEachRemaining(entry -> actualConfigMap.put(entry.getKey(), entry.getValue().textValue()));
        Map<String, String> expectConfig = new HashMap<>();
        expectConfig.putAll(config);
        expectConfig.put("name", CONNECTOR_NAME);
        Assert.assertEquals(expectConfig, actualConfigMap);
    }

    @Test
    public void testGetConnectorConfigAsExpect() throws JsonProcessingException {
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(restTemplate.getForObject(argumentCaptor.capture(), ArgumentMatchers.eq(String.class))).thenReturn("{\"name\":\"" + CONNECTOR_NAME + "\"}");
        Map<String, String> result = connectClusterRest.getConnectorConfig(MOCK_URL, CONNECTOR_NAME);
        Assert.assertEquals(CONNECTORS_MOCK_URL + "/" + CONNECTOR_NAME + "/config", argumentCaptor.getValue());
        Assert.assertEquals(CONNECTOR_NAME, result.get("name"));
    }

    @Test
    public void testPutConnectorConfigAsExpect() throws JsonProcessingException {
        Map<String, String> config = fakeConnectorConfig();
        connectClusterRest.putConnectorConfig(MOCK_URL, CONNECTOR_NAME, config);
        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HttpEntity> requestCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        Mockito.verify(restTemplate).exchange(urlCaptor.capture(), ArgumentMatchers.eq(HttpMethod.PUT), requestCaptor.capture(), ArgumentMatchers.eq(String.class));
        Assert.assertEquals(CONNECTORS_MOCK_URL + "/" + CONNECTOR_NAME + "/config", urlCaptor.getValue());
        HttpEntity httpEntity = requestCaptor.getValue();
        Assert.assertEquals(MediaType.APPLICATION_JSON, httpEntity.getHeaders().getContentType());
        JsonNode jsonNode = objectMapper.readTree((String) httpEntity.getBody());
        Assert.assertEquals(CONNECTOR_NAME, jsonNode.get("name").textValue());
        Map<String, String> actualConfigMap = new HashMap<>();
        jsonNode.fields().forEachRemaining(entry -> actualConfigMap.put(entry.getKey(), entry.getValue().textValue()));
        Map<String, String> expectConfig = new HashMap<>();
        expectConfig.putAll(config);
        expectConfig.put("name", CONNECTOR_NAME);
        Assert.assertEquals(expectConfig, actualConfigMap);
    }

    @Test
    public void testGetConnectorStatusAsExpect() {
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        connectClusterRest.getConnectorStatus(MOCK_URL, CONNECTOR_NAME);
        Mockito.verify(restTemplate).getForObject(argumentCaptor.capture(), ArgumentMatchers.eq(ConnectorStatusResponse.class));
        Assert.assertEquals(CONNECTORS_MOCK_URL + "/" + CONNECTOR_NAME + "/status", argumentCaptor.getValue());
    }

    @Test
    public void testDeleteConnector() {
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        connectClusterRest.deleteConnector(MOCK_URL, CONNECTOR_NAME);
        Mockito.verify(restTemplate).delete(argumentCaptor.capture());
        Assert.assertEquals(CONNECTORS_MOCK_URL + "/" + CONNECTOR_NAME, argumentCaptor.getValue());
    }

    private Map<String, String> fakeConnectorConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "cn.xdf.xcdc.connect.jdbc.JdbcSinkConnector");
        config.put("tasks.max", "3");
        config.put("topics", "cdc_tidb_local-ticdc_test-all_type_table_1");
        config.put("value.converter.schema.registry.url", "http://schema-registry:8081");
        config.put("tables", "sink_all_type_table_1");
        config.put("db.timezone", "Asia/Shanghai");
        config.put("name", CONNECTOR_NAME_NEW);
        config.put("connection.url", "jdbc:mysql://mysql-host:4000/test1?user=ticdc_test_usr&password=ticdc@test");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("tables.sink_all_type_table_1.fields.whitelist",
                "field_tinyint,field_bool,field_smallint,field_int,field_float,field_double,field_timestamp,field_bigint,"
                        + "field_mediumint,field_date,field_time,field_datetime,field_year,field_varchar,field_varbinary,field_bit,"
                        + "field_json,field_decimal,field_enum,field_set,field_tinytext,field_tinyblob,field_mediumtext,field_mediumblob,"
                        + "field_longtext,field_longblob,field_text,field_blob,field_char,field_binary,updatetime");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", "http://schema-registry:8081");
        config.put("tables.sink_all_type_table_1.fields.add", "ods_updatetime:${datetime}");
        return config;
    }

}
