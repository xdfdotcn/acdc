package cn.xdf.acdc.devops.service.process.datasystem.es;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.es.EsDataSystemConstant.Connector.Sink.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.es.EsDataSystemResourceConfigurationDefinition.Cluster;

@Service
public class EsDataSystemSinkConnectorServiceImpl extends AbstractDataSystemSinkConnectorService {

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private ConnectionService connectionService;

    @Override
    public void verifyDataSystemMetadata(
            final Long dataCollectionId
    ) {
        // do nothing
    }

    @Override
    public void beforeConnectorCreation(final Long dataCollectionId) {
        // do nothing
    }

    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService
                .getDetailByDataSystemTypeAndConnectorType(
                        DataSystemType.ELASTIC_SEARCH,
                        ConnectorType.SINK
                );

        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations()
                .forEach(each -> defaultConfigurations.put(
                        each.getName(),
                        each.getValue())
                );
        return defaultConfigurations;
    }

    @Override
    public Map<String, String> generateConnectorCustomConfiguration(
            final Long connectionId
    ) {
        ConnectionDetailDTO connectionDetail = connectionService
                .getDetailById(connectionId);

        DataSystemResourceDTO sourceDataCollection = dataSystemResourceService
                .getById(connectionDetail.getSourceDataCollectionId());

        DataSystemResourceDetailDTO sinkClusterDetail = dataSystemResourceService
                .getDetailParent(connectionDetail.getSinkDataCollectionId(),
                        DataSystemResourceType.ELASTIC_SEARCH_CLUSTER);

        DataSystemResourceDTO sinkIndex = dataSystemResourceService
                .getById(connectionDetail.getSinkDataCollectionId());

        // TODO  索引名使用SMT进行处理

        String username = getEsClusterConf(sinkClusterDetail, Cluster.USERNAME);
        String password = getEsClusterConf(sinkClusterDetail, Cluster.PASSWORD);
        String nodeServers = getEsClusterConf(sinkClusterDetail, Cluster.NODE_SERVERS);
        String indexName = sinkIndex.getName();

        String topic = sourceDataCollection.getKafkaTopicName();

        Map<String, String> configurations = new HashMap<>();

        // topic
        configurations.put(
                EsDataSystemConstant.Connector.Sink.Configuration.TOPICS,
                topic
        );
        // username
        configurations.put(
                EsDataSystemConstant.Connector.Sink.Configuration.CONNECTION_USERNAME,
                username
        );
        // password
        configurations.put(
                EsDataSystemConstant.Connector.Sink.Configuration.CONNECTION_PASSWORD,
                password
        );
        // url
        configurations.put(
                EsDataSystemConstant.Connector.Sink.Configuration.CONNECTION_URL,
                nodeServers
        );

        // whitelist
        configurations.put(
                Configuration.TRANSFORMS_REPLACEFIELD_WHITELIST,
                generateColumnWhitelistConfigurationValue(connectionDetail.getConnectionColumnConfigurations())
        );

        // renames
        configurations.put(
                EsDataSystemConstant.Connector.Sink.Configuration.TRANSFORMS_REPLACEFIELD_RENAME,
                generateColumnMappingConfigurationValue(connectionDetail.getConnectionColumnConfigurations())
        );

        // topic to index 
        configurations.put(Configuration.TRANSFORMS_TOPIC_TO_INDEX_REPLACEMENT, indexName);

        return configurations;
    }

    @Override
    public Set<ConfigurationDefinition<?>> getConnectorSpecificConfigurationDefinitions() {
        return Sets.newHashSet();
    }

    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Configuration.SENSITIVE_CONFIGURATION_NAMES;
    }

    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService
                .getDetailByDataSystemTypeAndConnectorType(DataSystemType.ELASTIC_SEARCH, ConnectorType.SINK);
    }

    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.ELASTIC_SEARCH;
    }

    private String getEsClusterConf(
            final DataSystemResourceDetailDTO esClusterDetail,
            final ConfigurationDefinition<?> definition
    ) {
        return esClusterDetail.getDataSystemResourceConfigurations()
                .get(definition.getName()).getValue();
    }
}
