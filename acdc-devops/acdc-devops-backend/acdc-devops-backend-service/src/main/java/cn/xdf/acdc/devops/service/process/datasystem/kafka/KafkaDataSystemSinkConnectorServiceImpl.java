package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.util.StringUtil;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DeletionMode;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemConstant.Connector.Sink.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Connector.Sink;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
public class KafkaDataSystemSinkConnectorServiceImpl extends AbstractDataSystemSinkConnectorService {

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private ConnectionService connectionService;

    @Override
    public void verifyDataSystemMetadata(final Long dataCollectionId) {

    }

    @Override
    public void beforeConnectorCreation(final Long dataCollectionId) {

    }

    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.KAFKA, ConnectorType.SINK);

        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }

    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final Long connectionId) {
        ConnectionDetailDTO connectionDetail = connectionService.getDetailById(connectionId);
        DataSystemResourceDTO sourceDataCollection = dataSystemResourceService.getById(connectionDetail.getSourceDataCollectionId());
        DataSystemResourceDTO sinkTopic = dataSystemResourceService.getById(connectionDetail.getSinkDataCollectionId());
        DataSystemResourceDetailDTO sinkKafkaClusterDetail = dataSystemResourceService.getDetailParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.KAFKA_CLUSTER);

        Map<String, String> configurations = new HashMap<>();
        configurations.put(Sink.Configuration.TOPICS, sourceDataCollection.getKafkaTopicName());
        configurations.put(Sink.Configuration.DESTINATIONS, sinkTopic.getName());

        // all configuration name with prefix destinations.
        configurations.putAll(super.generateDestinationsConfiguration(sinkTopic.getName(), connectionDetail.getConnectionColumnConfigurations()));

        // override logical deletion configuration to none
        configurations.put(
                AbstractDataSystemSinkConnectorService.DESTINATIONS_PREFIX + sinkTopic.getName() + AbstractDataSystemSinkConnectorService.DELETE_LOGICAL_MODE_SUFFIX,
                DeletionMode.NONE.name()
        );

        // kafka sink custom configuration
        KafkaConfigurationUtil.generateAdminClientConfiguration(sinkKafkaClusterDetail).forEach((name, value) -> {
            configurations.put(Configuration.KAFKA_CONFIG_PREFIX + name, value.toString());
        });

        // specific configuration
        configurations.putAll(generateSpecificConfiguration(connectionDetail.getSpecificConfiguration()));

        return configurations;
    }

    protected Map<String, String> generateSpecificConfiguration(final String specificConfigurationJson) {
        Map<String, String> connectionSpecificConfiguration = StringUtil.convertJsonStringToMap(specificConfigurationJson);
        String dataFormatType = connectionSpecificConfiguration.get(KafkaSinkConnectorSpecificConfigurationDefinition.Sink.DATA_FORMAT_TYPE.getName());
        switch (DataFormatType.valueOf(dataFormatType)) {
            case JSON:
                return new HashMap<>(Configuration.JSON_DATA_FORMAT_CONFIGURATION);
            case CDC_V1:
                return new HashMap<>(Configuration.CDC_V1_DATA_FORMAT_CONFIGURATION);
            case SCHEMA_LESS_JSON:
                return new HashMap<>(Configuration.SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION);
            default:
                throw new IllegalArgumentException(String.format("unsupported data format type %s", dataFormatType));
        }
    }

    @Override
    public Set<ConfigurationDefinition> getConnectorSpecificConfigurationDefinitions() {
        return KafkaSinkConnectorSpecificConfigurationDefinition.Sink.SPECIFIC_CONFIGURATION_DEFINITIONS;
    }

    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Configuration.SENSITIVE_CONFIGURATION_NAMES;
    }

    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.KAFKA, ConnectorType.SINK);
    }

    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.KAFKA;
    }
}
