package cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationGenerator;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import com.google.common.collect.HashBasedTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

@Service
@Slf4j
@Component
public class ConnectionColumnConfigurationGeneratorManager {
    
    private static final HashBasedTable<DataSystemType, DataSystemType, ConnectionColumnConfigurationGenerator> FIELD_MAPPING_SERVICE_MAP = HashBasedTable.create();
    
    @Autowired
    private ConnectionService connectionService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    public ConnectionColumnConfigurationGeneratorManager(final List<ConnectionColumnConfigurationGenerator> generators) {
        generators.forEach(it -> initGenerators(it.supportedSourceDataSystemTypes(), it.supportedSinkDataSystemTypes(), it));
    }
    
    private void initGenerators(
            final Set<DataSystemType> supportSrcDataSystemTypes,
            final Set<DataSystemType> supportSinkDataSystemTypes,
            final ConnectionColumnConfigurationGenerator connectionColumnConfigurationGenerator
    ) {
        // O(N^2) 笛卡尔, 同一个Generator, 需要支持多种类型的 source 和 sink 的组合
        for (DataSystemType supportSrcDataSystemType : supportSrcDataSystemTypes) {
            for (DataSystemType supportSinkDataSystemType : supportSinkDataSystemTypes) {
                FIELD_MAPPING_SERVICE_MAP.put(supportSrcDataSystemType, supportSinkDataSystemType, connectionColumnConfigurationGenerator);
            }
        }
    }
    
    /**
     * Get column configuration generator.
     *
     * @param sinkDataSystemType sink data system type
     * @param sourceDataSystemType source data system type
     * @return column configuration generator
     */
    public ConnectionColumnConfigurationGenerator getColumnConfigurationGenerator(
            final DataSystemType sourceDataSystemType,
            final DataSystemType sinkDataSystemType
    ) {
        ConnectionColumnConfigurationGenerator generator = FIELD_MAPPING_SERVICE_MAP
                .get(sourceDataSystemType, sinkDataSystemType);
        return generator;
    }
    
    /**
     * 根据已经保存的字段映射配置，按照数据库中查询的最新的表结构重新生成字段映射关系.
     *
     * @param connectionId connection id
     * @return connection column configuration list
     */
    public List<ConnectionColumnConfigurationDTO> generateConnectionColumnConfiguration(final Long connectionId) {
        ConnectionDTO connection = connectionService.getById(connectionId);
        
        Long sourceDataCollectionId = connection.getSourceDataCollectionId();
        Long sinkDataCollectionId = connection.getSinkDataCollectionId();
        
        DataSystemType sourceDataSystemType = dataSystemResourceService.getDataSystemType(sourceDataCollectionId);
        DataSystemType sinkDataSystemType = dataSystemResourceService.getDataSystemType(sinkDataCollectionId);
        
        DataCollectionDefinition sourceDataCollectionDefinition = dataSystemServiceManager.getDataSystemMetadataService(sourceDataSystemType)
                .getDataCollectionDefinition(sourceDataCollectionId);
        
        DataCollectionDefinition sinkDataCollectionDefinition = dataSystemServiceManager.getDataSystemMetadataService(sinkDataSystemType)
                .getDataCollectionDefinition(sinkDataCollectionId);
        
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations =
                connectionService.getDetailById(connectionId).getConnectionColumnConfigurations();
        
        return FIELD_MAPPING_SERVICE_MAP.get(sourceDataSystemType, sinkDataSystemType)
                .generateColumnConfiguration(sourceDataCollectionDefinition, sinkDataCollectionDefinition, connectionColumnConfigurations);
    }
    
    /**
     * Generate column configuration.
     *
     * @param sourceResourceId srcResourceId
     * @param sinkResourceId sinkResourceId
     * @return List
     */
    public List<ConnectionColumnConfigurationDTO> generateConnectionColumnConfiguration(
            final Long sourceResourceId,
            final Long sinkResourceId
    ) {
        DataSystemType sourceDataSystemType = dataSystemResourceService.getDataSystemType(sourceResourceId);
        DataSystemType sinkDataSystemType = dataSystemResourceService.getDataSystemType(sinkResourceId);
        
        DataCollectionDefinition sourceDataCollectionDefinition = dataSystemServiceManager.getDataSystemMetadataService(sourceDataSystemType)
                .getDataCollectionDefinition(sourceResourceId);
        
        DataCollectionDefinition sinkDataCollectionDefinition = dataSystemServiceManager.getDataSystemMetadataService(sinkDataSystemType)
                .getDataCollectionDefinition(sinkResourceId);
        
        // 2. diffing and sort
        return FIELD_MAPPING_SERVICE_MAP.get(sourceDataSystemType, sinkDataSystemType)
                .generateColumnConfiguration(sourceDataCollectionDefinition, sinkDataCollectionDefinition);
    }
}
