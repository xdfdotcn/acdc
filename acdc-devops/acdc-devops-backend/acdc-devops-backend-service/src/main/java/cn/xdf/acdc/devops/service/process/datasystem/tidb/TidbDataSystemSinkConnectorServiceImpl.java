package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.UrlUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
public class TidbDataSystemSinkConnectorServiceImpl extends AbstractDataSystemSinkConnectorService {

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private ConnectionService connectionService;

    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.TIDB;
    }

    @Override
    public void verifyDataSystemMetadata(final Long resourceId) {
        //keep empty
    }

    @Override
    public void beforeConnectorCreation(final Long resourceId) {
        //keep empty
    }

    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.TIDB, ConnectorType.SINK);

        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }

    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final Long connectionId) {
        ConnectionDetailDTO connectionDetail = connectionService.getDetailById(connectionId);
        DataSystemResourceDTO sourceDataCollection = dataSystemResourceService.getById(connectionDetail.getSourceDataCollectionId());
        DataSystemResourceDTO sinkTable = dataSystemResourceService.getById(connectionDetail.getSinkDataCollectionId());
        DataSystemResourceDetailDTO sinkClusterDetail = dataSystemResourceService.getDetailParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.TIDB_CLUSTER);
        DataSystemResourceDTO sinkDatabase = dataSystemResourceService.getParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.TIDB_DATABASE);

        Map<String, String> configurations = new HashMap<>();

        configurations.put(TidbDataSystemConstant.Connector.Sink.Configuration.TOPICS, sourceDataCollection.getKafkaTopicName());
        configurations.put(TidbDataSystemConstant.Connector.Sink.Configuration.DESTINATIONS, sinkTable.getName());
        configurations.put(TidbDataSystemConstant.Connector.Sink.Configuration.CONNECTION_URL, generateJDBCUrl(connectionDetail, sinkDatabase));
        configurations.put(TidbDataSystemConstant.Connector.Sink.Configuration.CONNECTION_USER, sinkClusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue());
        configurations.put(TidbDataSystemConstant.Connector.Sink.Configuration.CONNECTION_PASSWORD, sinkClusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue());

        // destination configurations
        configurations.putAll(super.generateDestinationsConfiguration(sinkTable.getName(), connectionDetail.getConnectionColumnConfigurations()));
        return configurations;
    }

    private String generateJDBCUrl(final ConnectionDetailDTO connectionDetail, final DataSystemResourceDTO database) {
        Long sinkInstanceId = connectionDetail.getSinkInstanceId();
        DataSystemResourceDetailDTO detailById = dataSystemResourceService.getDetailById(sinkInstanceId);
        Map<String, DataSystemResourceConfigurationDTO> sinkConfigs = detailById.getDataSystemResourceConfigurations();
        return UrlUtil.generateJDBCUrl(DataSystemType.MYSQL.name().toLowerCase(),
                sinkConfigs.get(CommonDataSystemResourceConfigurationDefinition.Endpoint.HOST_NAME).getValue(),
                Integer.parseInt(sinkConfigs.get(CommonDataSystemResourceConfigurationDefinition.Endpoint.PORT_NAME).getValue()),
                database.getName());
    }

    @Override
    public Set<ConfigurationDefinition> getConnectorSpecificConfigurationDefinitions() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return TidbDataSystemConstant.Connector.Sink.Configuration.SENSITIVE_CONFIGURATION_NAMES;
    }

    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.TIDB, ConnectorType.SINK);
    }
}
