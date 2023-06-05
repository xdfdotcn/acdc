package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.config.TopicProperties;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Connector.Source.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Connector.Source.Topic;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.Mysql;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.process.datasystem.util.SourceConnectorConfigurationUtil;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class MysqlDataSystemSourceConnectorServiceImpl implements DataSystemSourceConnectorService {
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private KafkaClusterService kafkaClusterService;
    
    @Autowired
    private TopicProperties topicProperties;
    
    @Autowired
    private KafkaHelperService kafkaHelperService;
    
    @Autowired
    private ConnectorClassService connectorClassService;
    
    @Autowired
    @Qualifier("mysqlDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Override
    public void verifyDataSystemMetadata(final Long dataCollectionId) {
        getDataSourceInstance(dataCollectionId);
    }
    
    private DataSystemResourceDetailDTO getDataSourceInstance(final long dataCollectionId) {
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_CLUSTER);
        List<DataSystemResourceDetailDTO> instances = dataSystemResourceService.getDetailChildren(
                cluster.getId(),
                DataSystemResourceType.MYSQL_INSTANCE,
                Instance.ROLE_TYPE.getName(),
                MysqlInstanceRoleType.DATA_SOURCE.name());
        
        if (instances.isEmpty()) {
            throw new ServerErrorException(
                    String.format("there is no instance with type data source in mysql cluster name: %s, id: %d", cluster.getName(), cluster.getId()));
        } else if (instances.size() > 1) {
            throw new ServerErrorException(
                    String.format("there is more than 1 instance with type data source in mysql cluster name: %s, id: %d", cluster.getName(), cluster.getId()));
        }
        
        return instances.get(0);
    }
    
    @Override
    public void beforeConnectorCreation(final Long dataCollectionId) {
    
    }
    
    @Override
    public void afterConnectorCreation(final Long dataCollectionId) {
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDTO database = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_DATABASE);
        
        // to prevent connector creation failure, we create topic in kafka cluster at the end
        createDebeziumSchemaHistoryTopic(cluster, database);
        createDebeziumSchemaChangeTopic(cluster, database);
    }
    
    private void createDebeziumSchemaHistoryTopic(final DataSystemResourceDTO cluster, final DataSystemResourceDTO database) {
        String topicName = generateDebeziumSchemaHistoryTopicName(cluster.getId(), database.getName());
        
        log.info("creating debezium schema history topic for mysql database, database id {} name {}, cluster id: {} name {}, topic name: {}",
                database.getId(), database.getName(), cluster.getId(), cluster.getName(), topicName);
        
        createDebeziumTopicIfAbsent(topicName,
                topicProperties.getSchemaHistory().getPartitions(),
                topicProperties.getSchemaHistory().getReplicationFactor(),
                topicProperties.getSchemaHistory().getConfigs());
        
        log.info("debezium schema history topic: {} created", topicName);
    }
    
    private String generateDebeziumSchemaHistoryTopicName(final long clusterId, final String databaseName) {
        return Joiner.on(CommonConstant.CABLE).join(
                Topic.SCHEMA_CHANGE_TOPIC_PREFIX,
                Configuration.CONNECTOR_NAME_PREFIX,
                clusterId,
                databaseName);
    }
    
    private void createDebeziumTopicIfAbsent(final String topicName, final int partitions, final short replicationFactor, final Map<String, String> topicConfigs) {
        createTopicInKafkaIfAbsent(topicName, partitions, replicationFactor, topicConfigs);
    }
    
    private void createTopicInKafkaIfAbsent(
            final String topic,
            final int partitions,
            final short replicationFactor,
            final Map<String, String> topicConfig) {
        KafkaClusterDTO kafkaCluster = kafkaClusterService.getACDCKafkaCluster();
        Map<String, Object> adminConfig = kafkaClusterService.getDecryptedAdminConfig(kafkaCluster.getId());
        
        kafkaHelperService.createTopic(
                topic,
                partitions,
                replicationFactor,
                topicConfig,
                adminConfig
        );
    }
    
    private void createDebeziumSchemaChangeTopic(final DataSystemResourceDTO cluster, final DataSystemResourceDTO database) {
        String topicName = generateDebeziumSchemaChangeTopicName(cluster.getId(), database.getName());
        
        log.info("creating debezium schema change topic for mysql database, database id {} name {}, cluster id: {} name {}, topic name: {}",
                database.getId(), database.getName(), cluster.getId(), cluster.getName(), topicName);
        
        createDebeziumTopicIfAbsent(topicName,
                topicProperties.getSchemaChange().getPartitions(),
                topicProperties.getSchemaChange().getReplicationFactor(),
                topicProperties.getSchemaChange().getConfigs());
        
        log.info("debezium schema change topic: {} created", topicName);
    }
    
    private String generateDebeziumSchemaChangeTopicName(final long clusterId, final String databaseName) {
        return generateDebeziumServerName(clusterId, databaseName);
    }
    
    @Override
    public String generateConnectorName(final Long dataCollectionId) {
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDTO database = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_DATABASE);
        
        return generateDebeziumServerName(cluster.getId(), database.getName());
    }
    
    @Override
    public String generateKafkaTopicName(final Long dataCollectionId) {
        DataSystemResourceDTO table = dataSystemResourceService.getById(dataCollectionId);
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDTO database = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.MYSQL_DATABASE);
        
        // kafka topic name's format is {debezium_server_name}-{table_name}
        return Joiner.on(CommonConstant.CABLE).join(generateDebeziumServerName(cluster.getId(), database.getName()), table.getName());
    }
    
    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final List<Long> dataCollectionIds) {
        Preconditions.checkState(!dataCollectionIds.isEmpty(), "dataCollectionIds can not be empty");
        
        long anyResourceId = dataCollectionIds.get(0);
        DataSystemResourceDetailDTO cluster = dataSystemResourceService.getDetailParent(anyResourceId, DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDTO database = dataSystemResourceService.getParent(anyResourceId, DataSystemResourceType.MYSQL_DATABASE);
        DataSystemResourceDetailDTO dataSourceInstance = getDataSourceInstance(anyResourceId);
        
        Map<String, String> customConfigurations = new HashMap<>();
        
        customConfigurations.putAll(generateConnectorDatabaseConfiguration(cluster, database, dataSourceInstance));
        customConfigurations.putAll(generateConnectorTableConfiguration(database, dataCollectionIds));
        customConfigurations.putAll(generateConnectorKafkaConfiguration());
        
        log.info("generated configurations for mysql source connector, table ids {}, database id: {} name {}, cluster id: {} name {}, configurations: {}",
                dataCollectionIds, database.getId(), database.getName(), cluster.getId(), cluster.getName(), customConfigurations);
        
        return customConfigurations;
    }
    
    private Map<String, String> generateConnectorDatabaseConfiguration(
            final DataSystemResourceDetailDTO cluster,
            final DataSystemResourceDTO database,
            final DataSystemResourceDetailDTO dataSourceInstance
    ) {
        Map<String, String> configurations = new HashMap<>();
        
        configurations.put(Configuration.DATABASE_USER, cluster.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue());
        configurations.put(Configuration.DATABASE_PASSWORD, cluster.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue());
        
        configurations.put(Configuration.DATABASE_HOSTNAME, dataSourceInstance.getDataSystemResourceConfigurations().get(Instance.HOST.getName()).getValue());
        configurations.put(Configuration.DATABASE_PORT, dataSourceInstance.getDataSystemResourceConfigurations().get(Instance.PORT.getName()).getValue());
        
        configurations.put(Configuration.DATABASE_SERVER_NAME, generateDebeziumServerName(cluster.getId(), database.getName()));
        
        configurations.put(Configuration.DATABASE_INCLUDE, database.getName());
        configurations.put(Configuration.DATABASE_HISTORY_KAFKA_TOPIC, generateDebeziumSchemaHistoryTopicName(cluster.getId(), database.getName()));
        
        return configurations;
    }
    
    private String generateDebeziumServerName(final long clusterId, final String databaseName) {
        return Joiner.on(CommonConstant.CABLE).join(Configuration.CONNECTOR_NAME_PREFIX, clusterId, databaseName);
    }
    
    private Map<String, String> generateConnectorTableConfiguration(
            final DataSystemResourceDTO database,
            final List<Long> tableIds
    ) {
        Map<String, String> configurations = new HashMap<>();
        
        // include tables
        List<DataSystemResourceDTO> tables = getTables(tableIds);
        String[] includeTableNames = new String[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            includeTableNames[i] = Joiner.on(CommonConstant.DOT).join(database.getName(), tables.get(i).getName());
        }
        configurations.put(Configuration.TABLE_INCLUDE_LIST, Joiner.on(CommonConstant.COMMA).join(includeTableNames));
        
        // message key
        String messageKeyColumnsValue = generateConnectorMessageKeyColumnsConfigurationValue(database, tableIds);
        configurations.put(Configuration.MESSAGE_KEY_COLUMNS, messageKeyColumnsValue);
        
        return configurations;
    }
    
    private List<DataSystemResourceDTO> getTables(final List<Long> tableIds) {
        List<DataSystemResourceDTO> tables = new ArrayList<>();
        tableIds.forEach(each -> tables.add(dataSystemResourceService.getById(each)));
        return tables;
    }
    
    private String generateConnectorMessageKeyColumnsConfigurationValue(
            final DataSystemResourceDTO database,
            final List<Long> tableIds
    ) {
        List<DataCollectionDefinition> tableDefinitions = new ArrayList();
        tableIds.forEach(each -> tableDefinitions.add(dataSystemMetadataService.getDataCollectionDefinition(each)));
        return SourceConnectorConfigurationUtil.generateConnectorMessageKeyColumnsConfigurationValue(database, tableDefinitions, Mysql.PK_INDEX_NAME);
    }
    
    // todo: kafka cluster route
    private Map<String, String> generateConnectorKafkaConfiguration() {
        KafkaClusterDTO kafkaCluster = kafkaClusterService.getACDCKafkaCluster();
        
        Map<String, String> securityConfigurations = convertJsonStringToMap(kafkaCluster.getSecurityConfiguration());
        
        Map<String, String> kafkaConfiguration = new HashMap<>();
        securityConfigurations.forEach((name, value) -> {
            kafkaConfiguration.put(Joiner.on(CommonConstant.DOT).join(Configuration.DATABASE_HISTORY_CONSUMER_PREFIX, name), value);
            kafkaConfiguration.put(Joiner.on(CommonConstant.DOT).join(Configuration.DATABASE_HISTORY_PRODUCER_PREFIX, name), value);
        });
        
        kafkaConfiguration.put(Configuration.DATABASE_HISTORY_KAFKA_BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers());
        return kafkaConfiguration;
    }
    
    private Map<String, String> convertJsonStringToMap(final String jsonString) {
        try {
            return objectMapper.readValue(jsonString, Map.class);
        } catch (JsonProcessingException e) {
            throw new ServerErrorException(String.format("error when convert json string '%s' to map", jsonString), e);
        }
    }
    
    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.MYSQL, ConnectorType.SOURCE);
        
        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }
    
    @Override
    public Set<String> getImmutableConfigurationNames() {
        return Configuration.IMMUTABLE_CONFIGURATION_NAMES;
    }
    
    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Configuration.SENSITIVE_CONFIGURATION_NAMES;
    }
    
    @Override
    public DataSystemResourceType getConnectorDataSystemResourceType() {
        return DataSystemResourceType.MYSQL_DATABASE;
    }
    
    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.MYSQL, ConnectorType.SOURCE);
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.MYSQL;
    }
}
