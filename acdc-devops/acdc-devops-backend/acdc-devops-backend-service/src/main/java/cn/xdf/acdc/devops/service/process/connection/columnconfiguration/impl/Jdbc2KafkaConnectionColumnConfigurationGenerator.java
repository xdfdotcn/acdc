package cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationGenerator;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class Jdbc2KafkaConnectionColumnConfigurationGenerator implements ConnectionColumnConfigurationGenerator {
    
    @Override
    public Set<DataSystemType> supportedSourceDataSystemTypes() {
        return Sets.immutableEnumSet(DataSystemType.MYSQL, DataSystemType.TIDB);
    }
    
    @Override
    public Set<DataSystemType> supportedSinkDataSystemTypes() {
        return Sets.immutableEnumSet(DataSystemType.KAFKA);
    }
    
    @Override
    public List<ConnectionColumnConfigurationDTO> generateColumnConfiguration(
            final DataCollectionDefinition sourceDataCollectionDefinition,
            final DataCollectionDefinition sinkDataCollectionDefinition
    ) {
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = sourceDataCollectionDefinition
                .getLowerCaseNameToDataFieldDefinitions().entrySet().stream()
                .map(entry -> new ConnectionColumnConfigurationDTO()
                        .setSourceColumnName(entry.getValue().getName())
                        .setSourceColumnType(entry.getValue().getType())
                        .setSourceColumnUniqueIndexNames(entry.getValue().getUniqueIndexNames())
                        .setSinkColumnName(entry.getValue().getName())
                        .setSinkColumnType(entry.getValue().getType())
                        .setSinkColumnUniqueIndexNames(entry.getValue().getUniqueIndexNames())
                )
                // 优先展示匹配到的字段，主键字排前面
                .sorted(Comparator.comparing(this::generateSequenceWhenNew))
                .collect(Collectors.toList());
        
        // react 前端组件需要存在id作为 unique key
        appendIdForConnectionColumnConfigurations(connectionColumnConfigurations, () -> 1);
        
        return connectionColumnConfigurations;
    }
}
