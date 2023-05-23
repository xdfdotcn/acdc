package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationConstant;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractDataSystemSinkConnectorService implements DataSystemSinkConnectorService {
    
    protected static final String DESTINATIONS_PREFIX = "destinations.";
    
    protected static final String ROW_FILTER_SUFFIX = ".row.filter";
    
    protected static final String FIELDS_MAPPING_SUFFIX = ".fields.mapping";
    
    protected static final String DELETE_LOGICAL_FIELD_NAME_SUFFIX = ".delete.logical.field.name";
    
    protected static final String DELETE_LOGICAL_MODE_SUFFIX = ".delete.mode";
    
    protected static final String DELETE_LOGICAL_FIELD_VALUE_DELETED_SUFFIX = ".delete.logical.field.value.deleted";
    
    protected static final String DELETE_LOGICAL_FIELD_VALUE_NORMAL_SUFFIX = ".delete.logical.field.value.normal";
    
    protected static final String FIELDS_WHITELIST_SUFFIX = ".fields.whitelist";
    
    protected static final String FIELDS_ADD_SUFFIX = ".fields.add";
    
    @Autowired
    private ConnectionService connectionService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    /**
     * Generate destinations configuration for sink connector.
     *
     * <p>
     * The configurations here are suitable for all kinds of sink which extends sink core abstract class in ACDC.
     * </p>
     *
     * @param destination destination
     * @param connectionColumnConfigurations connectionColumnConfigurations
     * @return configurations
     */
    protected Map<String, String> generateDestinationsConfiguration(final String destination, final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        Map<String, String> destinationsConfiguration = new HashMap<>();
        
        // field add
        String fieldAddConfigurationValue = generateExtensionColumnConfigurationValue(connectionColumnConfigurations);
        if (!Strings.isNullOrEmpty(fieldAddConfigurationValue)) {
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + FIELDS_ADD_SUFFIX, fieldAddConfigurationValue);
        }
        
        // field mapping
        String fieldMappingConfigurationValue = generateColumnMappingConfigurationValue(connectionColumnConfigurations);
        if (!Strings.isNullOrEmpty(fieldMappingConfigurationValue)) {
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + FIELDS_MAPPING_SUFFIX, fieldMappingConfigurationValue);
        }
        
        // white list
        String fieldWhiteListConfigurationValue = generateColumnWhitelistConfigurationValue(connectionColumnConfigurations);
        if (!Strings.isNullOrEmpty(fieldWhiteListConfigurationValue)) {
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + FIELDS_WHITELIST_SUFFIX, fieldWhiteListConfigurationValue);
        }
        
        // row filter expression
        String rowFilterExpressionConfigurationValue = generateDataRowFilterExpressionConfigurationValue(connectionColumnConfigurations);
        if (!Strings.isNullOrEmpty(rowFilterExpressionConfigurationValue)) {
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + ROW_FILTER_SUFFIX, rowFilterExpressionConfigurationValue);
        }
        
        // logical deletion
        Optional<LogicalDelColumn> logicalDelColumnOptional = generateLogicalDelColumnConfigurationValue(connectionColumnConfigurations);
        if (logicalDelColumnOptional.isPresent()) {
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + DELETE_LOGICAL_MODE_SUFFIX, DeletionMode.LOGICAL.name());
            LogicalDelColumn logicalDelColumn = logicalDelColumnOptional.get();
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + DELETE_LOGICAL_FIELD_NAME_SUFFIX,
                    logicalDelColumn.getLogicalDeletionColumnName());
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + DELETE_LOGICAL_FIELD_VALUE_DELETED_SUFFIX,
                    logicalDelColumn.getLogicalDeletionColumnValueDeleted());
            destinationsConfiguration.put(DESTINATIONS_PREFIX + destination + DELETE_LOGICAL_FIELD_VALUE_NORMAL_SUFFIX,
                    logicalDelColumn.getLogicalDeletionColumnValueNormal());
        }
        
        return destinationsConfiguration;
    }
    
    @Override
    public String generateConnectorName(final Long connectionId) {
        ConnectionDTO connection = connectionService.getById(connectionId);
        Long sourceDataCollectionId = connection.getSourceDataCollectionId();
        Long sinkDataCollectionId = connection.getSinkDataCollectionId();
        
        String sourceDataCollectionPathFormat = getPathFormatById(sourceDataCollectionId);
        String sinkDataCollectionPathFormat = getPathFormatById(sinkDataCollectionId);
        
        String connectorName = Joiner.on(Symbol.CABLE).join(SystemConstant.SINK, sinkDataCollectionPathFormat, SystemConstant.SOURCE, sourceDataCollectionPathFormat);
        return connectorName;
    }
    
    /**
     * Get data system resource path format by id.
     *
     * <p>
     * ordering rule: From the root node to the current node
     * </p>
     *
     * <p>
     * separator: "-"
     * </p>
     *
     * <p>
     * the first digit of the separator is the data system type
     * </p>
     *
     * <p>
     * eg: mysql-mysql_cluster1-database-table1,kafka-kafka_cluster1-topic1
     * </p>
     *
     * @param dataCollectionId data collection resource id
     * @return an ordered resource paths
     */
    protected String getPathFormatById(final Long dataCollectionId) {
        DataSystemResourceDTO dataCollectionDTO = dataSystemResourceService.getById(dataCollectionId);
        
        LinkedList<String> tempFormatList = new LinkedList<>();
        
        DataSystemResourceDTO rootResource = dataCollectionDTO;
        while (Objects.nonNull(rootResource.getParentResource())) {
            tempFormatList.push(rootResource.getName());
            rootResource = rootResource.getParentResource();
        }
        
        DataSystemType dataSystemType = rootResource.getDataSystemType();
        
        // special handling for root node (the root node could be itself,that means it has no parent resource)
        tempFormatList.push(String.valueOf(rootResource.getId()));
        tempFormatList.push(dataSystemType.name().toLowerCase());
        
        return Joiner.on(Symbol.CABLE).join(tempFormatList);
    }
    
    /**
     * Generate data row filter expression configuration value.
     *
     * @param connectionColumnConfigurations connection column configuration list
     * @return data row filter expression configuration value
     */
    private String generateDataRowFilterExpressionConfigurationValue(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        RowFilterExpress rowFilterExpress = RowFilterExpress.newRowFilterExpress();
        connectionColumnConfigurations.forEach(it -> rowFilterExpress.append(it));
        return rowFilterExpress.filterExpress();
    }
    
    /**
     * Generate source column whitelist configuration value.
     *
     * @param connectionColumnConfigurations connection column configuration list
     * @return source column whitelist configuration value
     */
    private String generateColumnWhitelistConfigurationValue(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        List<String> sourceFields = connectionColumnConfigurations.stream()
                .filter(it -> !ConnectionColumnConfigurationConstant.META_FIELD_SET.contains(it.getSourceColumnName()))
                .filter(it -> !isNone(it.getSourceColumnName()))
                .map(ConnectionColumnConfigurationDTO::getSourceColumnName).collect(Collectors.toList());
        
        return CollectionUtils.isEmpty(sourceFields)
                ? SystemConstant.EMPTY_STRING
                : Joiner.on(SystemConstant.Symbol.COMMA).join(sourceFields);
    }
    
    /**
     * Generate column mapping configuration value.
     *
     * @param connectionColumnConfigurations connection column configuration list
     * @return column mapping configuration value
     */
    private String generateColumnMappingConfigurationValue(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        List<String> mappings = connectionColumnConfigurations.stream()
                .filter(it -> !isNone(it.getSourceColumnName()))
                .filter(it -> !ConnectionColumnConfigurationConstant.META_FIELD_FILTER_SET.contains(it.getSourceColumnName()))
                .map(mapping -> new StringBuilder()
                        .append(mapping.getSourceColumnName())
                        .append(SystemConstant.Symbol.COLON)
                        .append(mapping.getSinkColumnName())
                        .toString()).collect(Collectors.toList());
        
        return CollectionUtils.isEmpty(mappings)
                ? SystemConstant.EMPTY_STRING
                : Joiner.on(SystemConstant.Symbol.COMMA).join(mappings);
    }
    
    /**
     * Generate logical deletion of column configuration value.
     *
     * @param connectionColumnConfigurations connection column configuration list
     * @return logical deletion of column configuration value
     */
    private Optional<LogicalDelColumn> generateLogicalDelColumnConfigurationValue(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        return findLogicalDelColumn(connectionColumnConfigurations);
    }
    
    /**
     * Generate extended column configuration value.
     *
     * @param connectionColumnConfigurations connection column configuration list
     * @return extended column configuration value
     */
    private String generateExtensionColumnConfigurationValue(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        List<String> extensionColumns = findExtensionColumn(connectionColumnConfigurations).stream()
                .map(it -> new StringBuilder().append(it.name).append(Symbol.COLON).append(it.value).toString())
                .collect(Collectors.toList());
        
        return CollectionUtils.isEmpty(extensionColumns)
                ? SystemConstant.EMPTY_STRING
                : Joiner.on(SystemConstant.Symbol.COMMA).join(extensionColumns);
    }
    
    private boolean isNone(final String fieldName) {
        return Strings.isNullOrEmpty(fieldName);
    }
    
    private Optional<LogicalDelColumn> findLogicalDelColumn(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        return connectionColumnConfigurations.stream()
                .filter(connectionColumnConfiguration -> Objects.equals(connectionColumnConfiguration.getSourceColumnName(), ConnectionColumnConfigurationConstant.META_LOGICAL_DEL))
                .map(LogicalDelColumn::new).findFirst();
    }
    
    private List<ExtensionColumn> findExtensionColumn(final List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations) {
        List<ExtensionColumn> extensionColumns = connectionColumnConfigurations.stream()
                .filter(configuration -> Objects.equals(configuration.getSourceColumnName(), ConnectionColumnConfigurationConstant.META_DATE_TIME))
                .map(ExtensionColumn::new
                ).collect(Collectors.toList());
        
        return CollectionUtils.isEmpty(extensionColumns)
                ? Collections.EMPTY_LIST : extensionColumns;
    }
    
    @Getter
    @Setter
    @NoArgsConstructor
    private static final class LogicalDelColumn {
        
        private static final String LOGICAL_DELETION_COLUMN_VALUE_DELETED = "1";
        
        private static final String LOGICAL_DELETION_COLUMN_VALUE_NORMAL = "0";
        
        private String logicalDeletionColumnName;
        
        private String logicalDeletionColumnValueDeleted;
        
        private String logicalDeletionColumnValueNormal;
        
        LogicalDelColumn(final ConnectionColumnConfigurationDTO connectionColumnConfiguration) {
            this.logicalDeletionColumnName = connectionColumnConfiguration.getSinkColumnName();
            this.logicalDeletionColumnValueDeleted = LOGICAL_DELETION_COLUMN_VALUE_DELETED;
            this.logicalDeletionColumnValueNormal = LOGICAL_DELETION_COLUMN_VALUE_NORMAL;
        }
    }
    
    @Getter
    @Setter
    @NoArgsConstructor
    private static final class ExtensionColumn {
        
        private String name;
        
        private String value;
        
        ExtensionColumn(final ConnectionColumnConfigurationDTO connectionColumnConfiguration) {
            this.name = connectionColumnConfiguration.getSinkColumnName();
            this.value = ConnectionColumnConfigurationConstant.META_DATE_TIME_VALUE;
        }
    }
    
    private static final class RowFilterExpress {
        
        private static final String EXPRESS_AND = " and ";
        
        private final StringBuilder filterExpress = new StringBuilder();
        
        private RowFilterExpress() {
        
        }
        
        private static RowFilterExpress newRowFilterExpress() {
            return new RowFilterExpress();
        }
        
        private RowFilterExpress append(final ConnectionColumnConfigurationDTO configuration) {
            String filterOperator = configuration.getFilterOperator();
            String filterValue = configuration.getFilterValue();
            String sourceFieldName = configuration.getSourceColumnName();
            if (Strings.isNullOrEmpty(filterOperator)
                    || Strings.isNullOrEmpty(filterOperator = filterOperator.trim())
                    || Strings.isNullOrEmpty(filterValue)
                    || Strings.isNullOrEmpty(filterValue = filterValue.trim())) {
                
                return this;
            }
            
            if (filterExpress.length() == 0) {
                filterExpress
                        .append(sourceFieldName)
                        .append(Symbol.BLANK)
                        .append(filterOperator)
                        .append(Symbol.BLANK).append(filterValue);
            } else {
                filterExpress
                        .append(EXPRESS_AND)
                        .append(sourceFieldName)
                        .append(Symbol.BLANK)
                        .append(filterOperator)
                        .append(Symbol.BLANK)
                        .append(filterValue);
            }
            
            return this;
        }
        
        private String filterExpress() {
            return this.filterExpress.toString();
        }
    }
}
