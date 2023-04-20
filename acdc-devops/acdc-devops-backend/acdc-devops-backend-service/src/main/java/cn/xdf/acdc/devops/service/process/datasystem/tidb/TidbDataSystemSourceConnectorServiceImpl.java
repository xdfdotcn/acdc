package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DefaultConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemConstant.Connector.Source.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemConstant.Connector.Source.Ticdc;
import cn.xdf.acdc.devops.service.process.datasystem.util.SourceConnectorConfigurationUtil;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class TidbDataSystemSourceConnectorServiceImpl implements DataSystemSourceConnectorService {

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    @Qualifier("tidbDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.TIDB;
    }

    @Override
    public void verifyDataSystemMetadata(final Long dataCollectionId) {
        //Keep empty.
    }

    @Override
    public void beforeConnectorCreation(final Long tableId) {
        DataSystemResourceDTO database = dataSystemResourceService.getParent(tableId, DataSystemResourceType.TIDB_DATABASE);
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(tableId, DataSystemResourceType.TIDB_CLUSTER);
        String ticdcTopicName = generateTicdcTopicName(cluster.getId(), database.getName());

        KafkaClusterDTO tidbKafkaCluster = kafkaClusterService.getTICDCKafkaCluster();
        kafkaTopicService.createTICDCTopicIfAbsent(ticdcTopicName, tidbKafkaCluster.getId(), database.getId());
    }

    private String generateTicdcTopicName(final Long clusterId, final String databaseName) {
        Preconditions.checkArgument(Objects.nonNull(clusterId));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(databaseName));
        return Joiner.on(CommonConstant.CABLE).join(Ticdc.TICDC_TOPIC_NAME_PREFIX, clusterId, databaseName);
    }

    @Override
    public void afterConnectorCreation(final Long dataCollectionId) {
        //keep empty
    }

    @Override
    public String generateConnectorName(final Long dataCollectionId) {
        DataSystemResourceDTO database = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.TIDB_DATABASE);
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.TIDB_CLUSTER);
        return generateDebeziumServerName(cluster.getId(), database.getName());
    }

    @Override
    public String generateKafkaTopicName(final Long dataCollectionId) {
        DataSystemResourceDTO table = dataSystemResourceService.getById(dataCollectionId);
        DataSystemResourceDTO cluster = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.TIDB_CLUSTER);
        DataSystemResourceDTO database = dataSystemResourceService.getParent(dataCollectionId, DataSystemResourceType.TIDB_DATABASE);

        // kafka topic name's format is {debezium_server_name}-{table_name}
        return Joiner.on(CommonConstant.CABLE).join(generateDebeziumServerName(cluster.getId(), database.getName()), table.getName());
    }

    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final List<Long> tableIds) {
        Preconditions.checkState(!tableIds.isEmpty(), "tableIds can not be empty");

        long anyTableId = tableIds.get(0);
        DataSystemResourceDetailDTO cluster = dataSystemResourceService.getDetailParent(anyTableId, DataSystemResourceType.TIDB_CLUSTER);
        DataSystemResourceDTO database = dataSystemResourceService.getParent(anyTableId, DataSystemResourceType.TIDB_DATABASE);
        KafkaClusterDTO ticdcKafkaCluster = kafkaClusterService.getTICDCKafkaCluster();

        String ticdcTopicName = generateTicdcTopicName(cluster.getId(), database.getName());

        Map<String, String> customConfigurations = new HashMap<>();
        String debeziumServerName = generateDebeziumServerName(cluster.getId(), database.getName());

        customConfigurations.put(Configuration.DATABASE_SERVER_NAME, debeziumServerName);

        customConfigurations.put(Configuration.DATABASE_INCLUDE, database.getName());

        List<DataCollectionDefinition> tables = getTables(tableIds);

        customConfigurations.put(Configuration.TABLE_INCLUDE_LIST, getTableNames(database, tables));

        customConfigurations.put(Configuration.MESSAGE_KEY_COLUMNS, getTableKeyColumns(database, tables));

        customConfigurations.put(Configuration.SOURCE_KAFKA_TOPIC, ticdcTopicName);

        customConfigurations.put(Configuration.SOURCE_KAFKA_GROUP_ID, debeziumServerName);

        customConfigurations.put(Configuration.SOURCE_KAFKA_BOOTSTRAP_SERVERS, ticdcKafkaCluster.getBootstrapServers());

        customConfigurations.putAll(generateConnectorKafkaSecurityConfiguration(ticdcKafkaCluster));

        return customConfigurations;
    }

    @SneakyThrows
    private Map<String, String> generateConnectorKafkaSecurityConfiguration(final KafkaClusterDTO ticdcKafkaCluster) {
        Map<String, String> adminConfig = objectMapper.readValue(ticdcKafkaCluster.getSecurityConfiguration(), Map.class);

        return adminConfig.entrySet().stream()
                .collect(Collectors.toMap(entry -> Configuration.SOURCE_KAFKA_PREFIX + entry.getKey(), Map.Entry::getValue));
    }

    private String getTableKeyColumns(
            final DataSystemResourceDTO database,
            final List<DataCollectionDefinition> tableDefinitions
    ) {
        return SourceConnectorConfigurationUtil.generateConnectorMessageKeyColumnsConfigurationValue(database,
                tableDefinitions, TidbDataSystemConstant.Metadata.Tidb.PK_INDEX_NAME);
    }

    private String getTableNames(final DataSystemResourceDTO database, final List<DataCollectionDefinition> tables) {
        String[] includeTableNames = new String[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            includeTableNames[i] = Joiner.on(CommonConstant.DOT).join(database.getName(), tables.get(i).getName());
        }
        return Joiner.on(CommonConstant.COMMA).join(includeTableNames);
    }

    private String generateDebeziumServerName(final long clusterId, final String databaseName) {
        return Joiner.on(CommonConstant.CABLE).join(Configuration.CONNECTOR_NAME_PREFIX, clusterId, databaseName);
    }

    private List<DataCollectionDefinition> getTables(final List<Long> tableIds) {
        List<DataCollectionDefinition> tables = new ArrayList<>();
        tableIds.forEach(each -> tables.add(dataSystemMetadataService.getDataCollectionDefinition(each)));
        return tables;
    }

    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService
                .getDetailByDataSystemTypeAndConnectorType(DataSystemType.TIDB, ConnectorType.SOURCE);

        return connectorClassDetail.getDefaultConnectorConfigurations().stream()
                .collect(Collectors.toMap(DefaultConnectorConfigurationDTO::getName, DefaultConnectorConfigurationDTO::getValue));
    }

    @Override
    public Set<String> getImmutableConfigurationNames() {
        return Configuration.IMMUTABLE_CONFIGURATION_NAMES;
    }

    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Configuration.SENSITIVE_CONFIGURATION_KEYS;
    }

    @Override
    public DataSystemResourceType getConnectorDataSystemResourceType() {
        return DataSystemResourceType.TIDB_DATABASE;
    }

    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.TIDB, ConnectorType.SOURCE);
    }
}
