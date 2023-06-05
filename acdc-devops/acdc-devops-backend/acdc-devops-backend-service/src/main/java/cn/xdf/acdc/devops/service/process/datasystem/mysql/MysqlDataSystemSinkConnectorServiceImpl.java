package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Connector.Sink.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.util.UrlUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class MysqlDataSystemSinkConnectorServiceImpl extends AbstractDataSystemSinkConnectorService {
    
    @Autowired
    private ConnectorClassService connectorClassService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private ConnectionService connectionService;
    
    @Override
    public void verifyDataSystemMetadata(final Long resourceId) {
        DataSystemResourceDetailDTO sinkClusterDetail = dataSystemResourceService.getDetailParent(resourceId, DataSystemResourceType.MYSQL_CLUSTER);
        getMasterInstance(sinkClusterDetail);
    }
    
    @Override
    public void beforeConnectorCreation(final Long resourceId) {
    
    }
    
    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.MYSQL, ConnectorType.SINK);
        
        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }
    
    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final Long connectionId) {
        ConnectionDetailDTO connectionDetail = connectionService.getDetailById(connectionId);
        DataSystemResourceDTO sourceDataCollection = dataSystemResourceService.getById(connectionDetail.getSourceDataCollectionId());
        DataSystemResourceDTO sinkTable = dataSystemResourceService.getById(connectionDetail.getSinkDataCollectionId());
        DataSystemResourceDetailDTO sinkClusterDetail = dataSystemResourceService.getDetailParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDTO sinkDatabase = dataSystemResourceService.getParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.MYSQL_DATABASE);
        DataSystemResourceDetailDTO sinkMasterInstance = getMasterInstance(sinkClusterDetail);
        
        Map<String, String> configurations = new HashMap<>();
        
        configurations.put(Configuration.TOPICS, sourceDataCollection.getKafkaTopicName());
        configurations.put(Configuration.DESTINATIONS, sinkTable.getName());
        configurations.put(Configuration.CONNECTION_URL, generateJDBCUrl(sinkMasterInstance, sinkDatabase));
        configurations.put(Configuration.CONNECTION_USER, sinkClusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue());
        configurations.put(Configuration.CONNECTION_PASSWORD, sinkClusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue());
        
        // destination configurations
        configurations.putAll(super.generateDestinationsConfiguration(sinkTable.getName(), connectionDetail.getConnectionColumnConfigurations()));
        return configurations;
    }
    
    private DataSystemResourceDetailDTO getMasterInstance(final DataSystemResourceDetailDTO clusterDetail) {
        List<DataSystemResourceDetailDTO> instances = dataSystemResourceService.getDetailChildren(
                clusterDetail.getId(),
                DataSystemResourceType.MYSQL_INSTANCE,
                Instance.ROLE_TYPE.getName(),
                MysqlInstanceRoleType.MASTER.name());
        
        if (instances.isEmpty()) {
            throw new ServerErrorException(
                    String.format("there is no instance with type master in mysql cluster name: %s, id: %d", clusterDetail.getName(), clusterDetail.getId()));
        } else if (instances.size() > 1) {
            throw new ServerErrorException(
                    String.format("there is more than 1 instance with type data source in mysql cluster name: %s, id: %d", clusterDetail.getName(), clusterDetail.getId()));
        }
        
        return instances.get(0);
    }
    
    private String generateJDBCUrl(final DataSystemResourceDetailDTO masterInstanceDetail, final DataSystemResourceDTO database) {
        String host = masterInstanceDetail.getDataSystemResourceConfigurations().get(Instance.HOST.getName()).getValue();
        int port = Integer.parseInt(masterInstanceDetail.getDataSystemResourceConfigurations().get(Instance.PORT.getName()).getValue());
        
        return UrlUtil.generateJDBCUrl(DataSystemType.MYSQL.name().toLowerCase(), host, port, database.getName());
    }
    
    @Override
    public Set<ConfigurationDefinition> getConnectorSpecificConfigurationDefinitions() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Configuration.SENSITIVE_CONFIGURATION_NAMES;
    }
    
    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.MYSQL, ConnectorType.SINK);
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.MYSQL;
    }
}
