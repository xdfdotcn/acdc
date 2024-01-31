package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Joiner;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationConstant;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Endpoint;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemConstant.Connector.Sink.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.FrontEnd;
import cn.xdf.acdc.devops.service.util.UrlUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.StarRocksHelperService;
import lombok.SneakyThrows;

@Service
public class StarRocksDataSystemSinkConnectorServiceImpl extends AbstractDataSystemSinkConnectorService {
    
    @Autowired
    @Qualifier("starRocksDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Autowired
    private ConnectorClassService connectorClassService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private ConnectionService connectionService;
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.STARROCKS;
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
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.STARROCKS, ConnectorType.SINK);
        
        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }
    
    @SneakyThrows
    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final Long connectionId) {
        ConnectionDetailDTO connectionDetail = connectionService.getDetailById(connectionId);
        DataSystemResourceDTO sourceDataCollection = dataSystemResourceService.getById(connectionDetail.getSourceDataCollectionId());
        DataSystemResourceDTO sinkDatabase = dataSystemResourceService.getParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.STARROCKS_DATABASE);
        DataSystemResourceDTO sinkTable = dataSystemResourceService.getById(connectionDetail.getSinkDataCollectionId());
        DataSystemResourceDetailDTO sinkClusterDetail = dataSystemResourceService.getDetailParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.STARROCKS_CLUSTER);
        
        Map<String, String> configurations = new HashMap<>();
        configurations.put(Configuration.TOPICS, sourceDataCollection.getKafkaTopicName());
        configurations.put(Configuration.DATABASE, sinkDatabase.getName());
        configurations.put(Configuration.TABLE, sinkTable.getName());
        configurations.put(Configuration.LOAD_URL, generateHttpUrl(sinkClusterDetail.getId()));
        configurations.put(Configuration.USERNAME,
                sinkClusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue());
        configurations.put(Configuration.PASSWORD,
                sinkClusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue());
        configurations.put(Configuration.TRANSFORMS, Configuration.TRANSFORMS_VALUE);
        configurations.put(Configuration.TRANSFORMS_DATE_TO_STRING_TYPE, Configuration.TRANSFORMS_DATE_TO_STRING_TYPE_VALUE);
        configurations.put(Configuration.TRANSFORMS_DATE_TO_STRING_ZONED_TIMESTAMP_FORMATTER, Configuration.TRANSFORMS_DATE_TO_STRING_ZONED_TIMESTAMP_FORMATTER_VALUE);
        configurations.put(Configuration.TRANSFORMS_UNWRAP_TYPE, Configuration.TRANSFORMS_UNWRAP_TYPE_VALUE);
        configurations.put(Configuration.TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE, Configuration.TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE_VALUE);
        configurations.put(Configuration.TRANSFORMS_UNWRAP_ADD_FIELDS, Configuration.TRANSFORMS_UNWRAP_ADD_FIELDS_VALUE);
        configurations.put(Configuration.TRANSFORMS_VALUE_MAPPER_SOURCE_TYPE, Configuration.TRANSFORMS_VALUE_MAPPER_SOURCE_TYPE_VALUE);
        configurations.put(Configuration.TRANSFORMS_VALUE_MAPPER_SOURCE_MAPPINGS, Configuration.TRANSFORMS_VALUE_MAPPER_SOURCE_MAPPINGS_VALUE);
        configurations.put(Configuration.TRANSFORMS_TRANSFORMS_VALUE_MAPPER_SOURCE_FIELD, Configuration.TRANSFORMS_TRANSFORMS_VALUE_MAPPER_SOURCE_FIELD_VALUE);
        configurations.put(Configuration.TRANSFORMS_REPLACE_FIELD_TYPE, Configuration.TRANSFORMS_REPLACE_FIELD_TYPE_VALUE);
        
        String sourceColumnWhitelist = generateColumnWhitelistConfigurationValue(connectionDetail.getConnectionColumnConfigurations());
        String sinkPropertiesColumns = generateSinkPropertiesColumns(connectionDetail.getConnectionColumnConfigurations());
        
        DataCollectionDefinition dataCollectionDefinition = dataSystemMetadataService.getDataCollectionDefinition(connectionDetail.getSinkDataCollectionId());
        String tableModel = (String) dataCollectionDefinition.getExtendProperties().get(StarRocksHelperService.TABLE_MODEL);
        
        if (Objects.equals(tableModel, StarRocksHelperService.TABLE_MODEL_PRIMARY_KEYS)) {
            sourceColumnWhitelist += Symbol.COMMA + Configuration.TRANSFORMS_TRANSFORMS_VALUE_MAPPER_SOURCE_FIELD_VALUE;
            sinkPropertiesColumns += Symbol.COMMA + Configuration.TRANSFORMS_TRANSFORMS_VALUE_MAPPER_SOURCE_FIELD_VALUE;
        }
        configurations.put(Configuration.TRANSFORMS_REPLACE_FIELD_WHITELIST, sourceColumnWhitelist);
        
        configurations.put(Configuration.TRANSFORMS_REPLACE_FIELD_RENAME,
                generateColumnMappingConfigurationValue(connectionDetail.getConnectionColumnConfigurations())
        );
        configurations.put(Configuration.SINK_COLUMNS, sinkPropertiesColumns);
        
        Optional<ConnectionColumnConfigurationDTO> logicalDelOptional =
                fieldOptional(connectionDetail.getConnectionColumnConfigurations(), ConnectionColumnConfigurationConstant.META_LOGICAL_DELETION);
        if (logicalDelOptional.isPresent()) {
            setLogicalDelConfig(configurations);
        }
        
        Optional<ConnectionColumnConfigurationDTO> offsetOptional =
                fieldOptional(connectionDetail.getConnectionColumnConfigurations(), ConnectionColumnConfigurationConstant.META_KAFKA_RECORD_OFFSET);
        if (offsetOptional.isPresent()) {
            setKafkaOffsetConfig(configurations);
        }
        
        return configurations;
    }
    
    private void setKafkaOffsetConfig(final Map<String, String> configurations) {
        configurations.put(Configuration.TRANSFORMS, Configuration.TRANSFORMS_VALUE_4_LOGICAL_DELETION);
        configurations.put(Configuration.TRANSFORMS_INSERT_FIELD_TYPE, Configuration.TRANSFORMS_INSERT_FIELD_TYPE_VALUE);
        configurations.put(Configuration.TRANSFORMS_INSERT_FIELD_OFFSET_FIELD, Configuration.TRANSFORMS_INSERT_FIELD_OFFSET_FIELD_VALUE);
    }
    
    private void setLogicalDelConfig(final Map<String, String> configurations) {
        configurations.put(Configuration.TRANSFORMS_VALUE_MAPPER_SOURCE_MAPPINGS, Configuration.TRANSFORMS_VALUE_MAPPER_SOURCE_MAPPINGS_VALUE_4_LOGICAL_DELETION);
    }
    
    private Optional<ConnectionColumnConfigurationDTO> fieldOptional(final List<ConnectionColumnConfigurationDTO> columnConfigs, final String fieldName) {
        return columnConfigs.stream().filter(
                connectionColumnConfiguration -> Objects.equals(connectionColumnConfiguration.getSourceColumnName(), fieldName)
        ).findFirst();
    }
    
    /**
     * Generate sink properties columns.
     *
     * @param connectionColumnConfigurations connection column configuration list
     * @return sink properties columns
     */
    protected String generateSinkPropertiesColumns(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        List<String> sinkFields = connectionColumnConfigurations.stream()
                .filter(it -> !ConnectionColumnConfigurationConstant.META_OP.equals(it.getSourceColumnName()))
                .filter(it -> !isNone(it.getSourceColumnName()) && !isNone(it.getSinkColumnName()))
                .map(ConnectionColumnConfigurationDTO::getSinkColumnName).collect(Collectors.toList());
        
        return CollectionUtils.isEmpty(sinkFields)
                ? SystemConstant.EMPTY_STRING
                : Joiner.on(Symbol.COMMA).join(sinkFields);
    }
    
    private String generateHttpUrl(final Long clusterId) {
        final List<DataSystemResourceDetailDTO> frontEnds = dataSystemResourceService.getDetailChildren(clusterId, DataSystemResourceType.STARROCKS_FRONTEND);
        final List<String> ipHostList = frontEnds.stream().map(frontEnd -> {
            String host = frontEnd.getDataSystemResourceConfigurations().get(Endpoint.HOST_NAME).getValue();
            String httpPort = frontEnd.getDataSystemResourceConfigurations().get(FrontEnd.HTTP_PORT_NAME).getValue();
            return UrlUtil.generateHttpUrl(host, Integer.parseInt(httpPort));
        }).collect(Collectors.toList());
        return String.join(CommonConstant.COMMA, ipHostList);
    }
    
    @Override
    public Set<ConfigurationDefinition<?>> getConnectorSpecificConfigurationDefinitions() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Configuration.SENSITIVE_CONFIGURATION_NAMES;
    }
    
    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.STARROCKS, ConnectorType.SINK);
    }
}
