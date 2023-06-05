package cn.xdf.acdc.devops.service.process.connection.columnconfiguration;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.util.StringUtil;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.Mysql;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemConstant.Metadata.Tidb;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.springframework.util.CollectionUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface ConnectionColumnConfigurationGenerator {
    
    int IS_MATCH = 1;
    
    int NOT_MATCH = 2;
    
    int PRIMARY_INDEX_ORDER_VALUE = 1;
    
    int UNIQUE_INDEX_ORDER_VALUE = 2;
    
    int NORMAL_FIELD_INDEX_ORDER_VALUE = 3;
    
    int ACDC_META_FIELD_ORDER_VALUE = 4;
    
    int EMPTY_FIELD_INDEX_ORDER_VALUE = 5;
    
    Set<String> PRIMARY_INDEX_NAME_SET = Sets.newHashSet(
            Mysql.PK_INDEX_NAME,
            Tidb.PK_INDEX_NAME
    );
    
    String ROW_ID_PREFIX = "row_id_";
    
    /**
     * Source data system type.
     *
     * @return Set
     */
    Set<DataSystemType> supportedSourceDataSystemTypes();
    
    /**
     * Sink data system type.
     *
     * @return Set
     */
    Set<DataSystemType> supportedSinkDataSystemTypes();
    
    /**
     * Whether it is a metadata field.
     *
     * @param filedName filed name
     * @return true: acdc meta filed
     */
    default boolean isMetaFiled(String filedName) {
        return ConnectionColumnConfigurationConstant.META_FIELD_SET.contains(filedName);
    }
    
    /**
     * Whether the configuration has missing.
     *
     * @param sourceDataCollectionDefinition current source data collection definition
     * @param columnConfiguration current column configuration
     * @return true: configuration change occurs
     */
    default boolean maybeSourceColumnMissing(
            DataCollectionDefinition sourceDataCollectionDefinition,
            ConnectionColumnConfigurationDTO columnConfiguration) {
        String sourceColumnName = toLowerCaseName(columnConfiguration.getSourceColumnName());
        Map<String, DataFieldDefinition> sourceNameToDataFieldDefinitions = sourceDataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions();
        return !isNone(sourceColumnName)
                && !isMetaFiled(sourceColumnName)
                && !sourceNameToDataFieldDefinitions.containsKey(sourceColumnName);
    }
    
    /**
     * Converts  name to lowercase.
     *
     * @param name name
     * @return lowercase  name
     */
    default String toLowerCaseName(String name) {
        if (isNone(name)) {
            return name;
        }
        return name.toLowerCase();
    }
    
    /**
     * Generate column configuration.
     *
     * @param sourceDataCollectionDefinition source data collection definition
     * @param sinkDataCollectionDefinition sink data collection definition
     * @return List
     */
    default List<ConnectionColumnConfigurationDTO> generateColumnConfiguration(
            DataCollectionDefinition sourceDataCollectionDefinition,
            DataCollectionDefinition sinkDataCollectionDefinition
    ) {
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = sinkDataCollectionDefinition
                .getLowerCaseNameToDataFieldDefinitions().entrySet().stream()
                .map(entry -> buildColumnConfiguration(entry.getValue(), sourceDataCollectionDefinition))
                // 优先展示匹配到的字段，主键字排前面
                .sorted(Comparator.comparing(this::generateSequenceWhenNew))
                .collect(Collectors.toList());
        
        // react 前端组件需要存在id作为 unique key
        appendIdForConnectionColumnConfigurations(connectionColumnConfigurations, () -> 1);
        
        return connectionColumnConfigurations;
    }
    
    /**
     * Diffing field.
     *
     * @param sourceDataCollectionDefinition source data collection definition
     * @param sinkDataCollectionDefinition sink data collection definition
     * @param alreadyExistConnectionColumnConfigurations connection column configurations
     * @return connection column configurations
     */
    default List<ConnectionColumnConfigurationDTO> generateColumnConfiguration(
            DataCollectionDefinition sourceDataCollectionDefinition,
            DataCollectionDefinition sinkDataCollectionDefinition,
            List<ConnectionColumnConfigurationDTO> alreadyExistConnectionColumnConfigurations
    ) {
        Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameToColumnConfiguration =
                alreadyExistConnectionColumnConfigurations.stream().collect(
                        Collectors.toMap(it -> toLowerCaseName(it.getSinkColumnName()), it -> it));
        
        List<ConnectionColumnConfigurationDTO> newColumnConfigurations = sinkDataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions().entrySet().stream()
                .map(entry -> buildColumnConfiguration(entry.getValue(), sourceDataCollectionDefinition, sinkColumnNameToColumnConfiguration))
                // 优先展示匹配到的字段，主键字排前面
                .sorted(Comparator.comparing(this::generateSequenceWhenNew))
                .collect(Collectors.toList());
        
        // react 前端组件需要存在id作为 unique key
        appendIdForConnectionColumnConfigurations(newColumnConfigurations, () -> 1);
        
        return newColumnConfigurations;
    }
    
    /**
     * Get sequence fro each mappings.
     *
     * @param connectionColumnConfiguration connectionColumnConfiguration
     * @return sequence
     */
    default String generateSequenceWhenNew(ConnectionColumnConfigurationDTO connectionColumnConfiguration) {
        DataFieldDefinition sourceDataFieldDefinition = new DataFieldDefinition(
                connectionColumnConfiguration.getSourceColumnName(),
                connectionColumnConfiguration.getSourceColumnType(),
                connectionColumnConfiguration.getSourceColumnUniqueIndexNames()
        );
        
        int columnMatchingRate = calculateColumnMatchingRate(connectionColumnConfiguration);
        int sourceFieldDefinitionOrderValue = calculateFieldDefinitionOrderValue(sourceDataFieldDefinition);
        String sourceColumnName = connectionColumnConfiguration.getSourceColumnName();
        return new StringBuilder()
                .append(columnMatchingRate)
                .append(sourceFieldDefinitionOrderValue)
                .append(sourceColumnName)
                .toString();
    }
    
    /**
     * Get sequence fro each mappings.
     *
     * @param connectionColumnConfiguration connectionColumnConfiguration
     * @return sequence
     */
    default String generateSequenceWhenEdit(ConnectionColumnConfigurationDTO connectionColumnConfiguration) {
        DataFieldDefinition sourceDataFieldDefinition = new DataFieldDefinition(
                connectionColumnConfiguration.getSourceColumnName(),
                connectionColumnConfiguration.getSourceColumnType(),
                connectionColumnConfiguration.getSourceColumnUniqueIndexNames()
        );
        
        int sourceFieldDefinitionOrderValue = calculateFieldDefinitionOrderValue(sourceDataFieldDefinition);
        String sourceColumnName = connectionColumnConfiguration.getSourceColumnName();
        return new StringBuilder()
                .append(sourceFieldDefinitionOrderValue)
                .append(sourceColumnName)
                .toString();
    }
    
    /**
     * Build fieldMapping with exist config.
     *
     * @param sinkDataFieldDefinition sink field definition
     * @param sourceDataCollectionDefinition source data collection definition
     * @param sinkColumnNameAndColumnConfigurationMapping sink filed name to column configuration
     * @return FieldMappingDTO
     */
    default ConnectionColumnConfigurationDTO buildColumnConfiguration(
            DataFieldDefinition sinkDataFieldDefinition,
            DataCollectionDefinition sourceDataCollectionDefinition,
            Map<String, ConnectionColumnConfigurationDTO> sinkColumnNameAndColumnConfigurationMapping
    ) {
        String sinkFieldName = toLowerCaseName(sinkDataFieldDefinition.getName());
        DataFieldDefinition sinkFieldDefinition = sinkDataFieldDefinition;
        
        ConnectionColumnConfigurationDTO columnConfiguration = sinkColumnNameAndColumnConfigurationMapping.get(sinkFieldName);
        
        boolean maybeSinkAddNewColumn = Objects.isNull(columnConfiguration);
        
        // source 字段变更,或者sink新增加了字段
        if (maybeSinkAddNewColumn || maybeSourceColumnMissing(sourceDataCollectionDefinition, columnConfiguration)
        ) {
            // TODO 如果字段映射发生变更,比如: sink增加字段,同时也能匹配到source的字段,是否自动匹配?,还是让用户感知到字段的变化,自己手动处理?
            // TODO 目前逻辑: 需要用户感知到字段变化,自己手动处理字段映射
            return new ConnectionColumnConfigurationDTO()
                    .setSinkColumnName(sinkFieldDefinition.getName())
                    .setSinkColumnType(sinkFieldDefinition.getType())
                    .setSinkColumnUniqueIndexNames(sinkDataFieldDefinition.getUniqueIndexNames());
        }
        
        // 字段映射关系无改变,更新对应的字段类型和索引
        String matchedSourceFieldName = toLowerCaseName(columnConfiguration.getSourceColumnName());
        DataFieldDefinition sourceFieldDefinition = isNone(matchedSourceFieldName) || isMetaFiled(matchedSourceFieldName)
                ? new DataFieldDefinition(
                columnConfiguration.getSourceColumnName(),
                columnConfiguration.getSourceColumnType(),
                columnConfiguration.getSourceColumnUniqueIndexNames())
                : sourceDataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions().get(matchedSourceFieldName);
        
        return new ConnectionColumnConfigurationDTO()
                .setSourceColumnName(sourceFieldDefinition.getName())
                .setSourceColumnType(sourceFieldDefinition.getType())
                .setSourceColumnUniqueIndexNames(sourceFieldDefinition.getUniqueIndexNames())
                .setSinkColumnName(sinkFieldDefinition.getName())
                .setSinkColumnType(sinkFieldDefinition.getType())
                .setSinkColumnUniqueIndexNames(sinkDataFieldDefinition.getUniqueIndexNames());
    }
    
    /**
     * Build connection column configuration.
     *
     * @param sinkDataFieldDefinition sink data field definition
     * @param sourceDataCollectionDefinition source data collection definition
     * @return ConnectionColumnConfigurationDTO
     */
    default ConnectionColumnConfigurationDTO buildColumnConfiguration(
            final DataFieldDefinition sinkDataFieldDefinition,
            final DataCollectionDefinition sourceDataCollectionDefinition
    ) {
        Map<String, DataFieldDefinition> sourceFiledDefinitionMapping = sourceDataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions();
        String sinkFieldName = toLowerCaseName(sinkDataFieldDefinition.getName());
        DataFieldDefinition sinkFieldDefinition = sinkDataFieldDefinition;
        DataFieldDefinition sourceFieldDefinition = sourceFiledDefinitionMapping.get(sinkFieldName);
        
        boolean isMatched = Objects.nonNull(sourceFieldDefinition);
        
        return isMatched ? new ConnectionColumnConfigurationDTO()
                .setSourceColumnName(sourceFieldDefinition.getName())
                .setSourceColumnType(sourceFieldDefinition.getType())
                .setSourceColumnUniqueIndexNames(sourceFieldDefinition.getUniqueIndexNames())
                .setSinkColumnName(sinkFieldDefinition.getName())
                .setSinkColumnType(sinkFieldDefinition.getType())
                .setSinkColumnUniqueIndexNames(sinkDataFieldDefinition.getUniqueIndexNames())
                : new ConnectionColumnConfigurationDTO()
                .setSinkColumnName(sinkFieldDefinition.getName())
                .setSinkColumnType(sinkFieldDefinition.getType())
                .setSinkColumnUniqueIndexNames(sinkDataFieldDefinition.getUniqueIndexNames());
    }
    
    /**
     * Append ID for each item. TODO 1. 编号应为受到,前端的组件bug导致,id为0开始则编辑保存按钮无效 TODO 2. 编号如果从1开始则编辑的时候,会出现同时编辑两行的问题 TODO 3. 如果编号可以使用一个字符串前缀,然后再增加上编号就可以了
     *
     * @param mappings mappings
     * @param beginId begin id
     */
    default void appendIdForConnectionColumnConfigurations(List<ConnectionColumnConfigurationDTO> mappings, Supplier<Integer> beginId) {
        int begin = beginId.get();
        for (ConnectionColumnConfigurationDTO mapping : mappings) {
            long rowIndex = begin++;
            mapping.setRowId(ROW_ID_PREFIX + rowIndex);
            mapping.setId(Long.valueOf(rowIndex));
        }
    }
    
    /**
     * Calculate matching rate for column configuration.
     *
     * @param columnConfiguration column configuration
     * @return matching rate
     */
    default int calculateColumnMatchingRate(final ConnectionColumnConfigurationDTO columnConfiguration) {
        return StringUtil.equalsIgnoreCase(columnConfiguration.getSourceColumnName(), columnConfiguration.getSinkColumnName())
                ? IS_MATCH
                : NOT_MATCH;
    }
    
    /**
     * Whether it is a none field.
     *
     * @param filedName filed name
     * @return true: none file name
     */
    default boolean isNone(String filedName) {
        return Strings.isNullOrEmpty(filedName);
    }
    
    /**
     * Calculate sort value for field definition.
     *
     * @param fieldDefinition field definition
     * @return sort value
     */
    default Integer calculateFieldDefinitionOrderValue(final DataFieldDefinition fieldDefinition) {
        Set<String> uniqueIndexNames = fieldDefinition.getUniqueIndexNames();
        String fieldName = fieldDefinition.getName();
        
        // empty field
        if (Strings.isNullOrEmpty(fieldName)) {
            return EMPTY_FIELD_INDEX_ORDER_VALUE;
        }
        
        // acdc meta field
        if (ConnectionColumnConfigurationConstant.META_FIELD_SET.contains(fieldName)) {
            return ACDC_META_FIELD_ORDER_VALUE;
        }
        
        // normal field
        if (CollectionUtils.isEmpty(uniqueIndexNames)) {
            return NORMAL_FIELD_INDEX_ORDER_VALUE;
        }
        
        // primary index field
        for (String primaryKey : PRIMARY_INDEX_NAME_SET) {
            if (uniqueIndexNames.contains(primaryKey)) {
                return PRIMARY_INDEX_ORDER_VALUE;
            }
        }
        
        // unique index field
        return UNIQUE_INDEX_ORDER_VALUE;
    }
}
