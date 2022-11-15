package cn.xdf.acdc.devops.biz.connect;

import cn.xdf.acdc.devops.biz.connect.response.ConnectorStatusResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.annotation.Timed;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ConnectClusterRest {

    private static final String CONNECTORS_URL = "{0}/connectors";

    private static final String CONNECTOR_URL = "{0}/connectors/{1}";

    private static final String CONNECTOR_CONFIG_URL = "{0}/connectors/{1}/config";

    private static final String CONNECTOR_CONFIG_VALIDATE_URL = "{0}/connector-plugins/{1}/config/validate";

    private static final String CONNECTOR_STATUS_URL = "{0}/connectors/{1}/status";

    private static final String FAILED_CONNECTOR_TASK_RESTART_URL = "{0}/connectors/{1}/restart?includeTasks=true&onlyFailed=true";

    private static final String CONNECTOR_CONFIG_KEY_NAME = "name";

    private static final String CONNECTOR_CONFIG_KEY_CONFIG = "config";

    private static final String CONNECTOR_CONFIG_KEY_CONNECTOR_TYPE = "connector.class";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final RestTemplate restTemplate;

    public ConnectClusterRest(final RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Get all connectors by connect cluster id.
     *
     * @param connectClusterUrl connect cluster url
     * @return connector list
     * @throws ResourceAccessException connection refused or timeout
     */
    @Timed
    public List<String> getAllConnectorByClusterUrl(final String connectClusterUrl) throws ResourceAccessException {
        String url = MessageFormat.format(CONNECTORS_URL, connectClusterUrl);
        List<String> connectors = restTemplate.getForObject(url, List.class);
        return connectors == null ? new ArrayList<>() : connectors;
    }

    /**
     * Create connector.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connector name
     * @param connectorConfig   connector config
     * @throws ResourceAccessException  connection refused or timeout
     * @throws HttpClientErrorException check param error return HttpStatus.BAD_REQUEST, has existed return HttpStatus.CONFLICT
     */
    @Timed
    public void createConnector(final String connectClusterUrl, final String connectorName, final Map<String, String> connectorConfig) throws ResourceAccessException, HttpClientErrorException {
        String url = MessageFormat.format(CONNECTORS_URL, connectClusterUrl);
        ObjectNode connectorJsonObject = objectMapper.createObjectNode();
        connectorJsonObject.put(CONNECTOR_CONFIG_KEY_NAME, connectorName);
        connectorJsonObject.set(CONNECTOR_CONFIG_KEY_CONFIG, getConfigObjectNode(connectorConfig, connectorName));
        HttpEntity<String> request = new HttpEntity<>(connectorJsonObject.toString(), getJsonHeaders());

        restTemplate.postForObject(url, request, String.class);
    }

    /**
     * Get connector config.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connect name
     * @return connector config
     * @throws ResourceAccessException  connection refused or timeout
     * @throws JsonProcessingException  return json parsing exception
     * @throws HttpClientErrorException HttpStatus.NOT_FOUND
     */
    @Timed
    public Map<String, String> getConnectorConfig(final String connectClusterUrl, final String connectorName) throws ResourceAccessException, JsonProcessingException, HttpClientErrorException {
        String url = MessageFormat.format(CONNECTOR_CONFIG_URL, connectClusterUrl, connectorName);
        String connectorConfigStr = restTemplate.getForObject(url, String.class);
        JsonNode connectorConfigJsonNode = objectMapper.readTree(connectorConfigStr);
        return objectMapper.convertValue(connectorConfigJsonNode, HashMap.class);
    }

    /**
     * Update connector config.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connector name
     * @param config            config
     * @throws ResourceAccessException  connection refused or timeout
     * @throws HttpClientErrorException check param error or not found return HttpStatus.BAD_REQUEST
     */
    @Timed
    public void putConnectorConfig(final String connectClusterUrl, final String connectorName, final Map<String, String> config) throws ResourceAccessException, HttpClientErrorException {
        String url = MessageFormat.format(CONNECTOR_CONFIG_URL, connectClusterUrl, connectorName);

        ObjectNode personJsonObject = getConfigObjectNode(config, connectorName);
        HttpEntity<String> request = new HttpEntity<>(personJsonObject.toString(), getJsonHeaders());

        restTemplate.exchange(url, HttpMethod.PUT, request, String.class);
    }

    /**
     * Get connector status.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connector name
     * @return connector status
     * @throws ResourceAccessException  connection refused or timeout
     * @throws HttpClientErrorException HttpStatus.NOT_FOUND
     */
    @Timed
    public ConnectorStatusResponse getConnectorStatus(final String connectClusterUrl, final String connectorName) throws ResourceAccessException, HttpClientErrorException {
        String url = MessageFormat.format(CONNECTOR_STATUS_URL, connectClusterUrl, connectorName);
        return restTemplate.getForObject(url, ConnectorStatusResponse.class);
    }

    /**
     * Restart connector or tasks once a connector or task failed.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connector name
     * @throws ResourceAccessException  connection refused or timeout
     * @throws HttpClientErrorException HttpStatus.NOT_FOUND
     */
    @Timed
    public void restartConnectorAndTasks(final String connectClusterUrl, final String connectorName) throws ResourceAccessException, HttpClientErrorException {
        String url = MessageFormat.format(FAILED_CONNECTOR_TASK_RESTART_URL, connectClusterUrl, connectorName);
        restTemplate.postForObject(url, new HttpEntity<>(null, getJsonHeaders()), String.class);
    }

    /**
     * Delete connector.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connector name
     * @throws ResourceAccessException  connection refused or timeout
     * @throws HttpClientErrorException HttpStatus.NOT_FOUND
     */
    @Timed
    public void deleteConnector(final String connectClusterUrl, final String connectorName) throws ResourceAccessException, HttpClientErrorException {
        String url = MessageFormat.format(CONNECTOR_URL, connectClusterUrl, connectorName);
        restTemplate.delete(url);
    }

    /**
     * Valid connector config.
     *
     * @param connectClusterUrl connect cluster url
     * @param connectorName     connector name
     * @param config            config
     * @throws ResourceAccessException  connection refused or timeout
     * @throws HttpClientErrorException check param error return HttpStatus.BAD_REQUEST
     */
    @Timed
    public void validConnectorConfig(final String connectClusterUrl, final String connectorName, final Map<String, String> config) {
        String url = MessageFormat.format(CONNECTOR_CONFIG_VALIDATE_URL, connectClusterUrl, config.get(CONNECTOR_CONFIG_KEY_CONNECTOR_TYPE));
        ObjectNode personJsonObject = getConfigObjectNode(config, connectorName);
        HttpEntity<String> request = new HttpEntity<>(personJsonObject.toString(), getJsonHeaders());
        restTemplate.exchange(url, HttpMethod.PUT, request, String.class);
    }

    private ObjectNode getConfigObjectNode(final Map<String, String> connectorConfig, final String connectorName) {
        ObjectNode configObjectNode = objectMapper.createObjectNode();
        connectorConfig.forEach(configObjectNode::put);
        configObjectNode.put(CONNECTOR_CONFIG_KEY_NAME, connectorName);
        return configObjectNode;
    }

    private HttpHeaders getJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }
}
