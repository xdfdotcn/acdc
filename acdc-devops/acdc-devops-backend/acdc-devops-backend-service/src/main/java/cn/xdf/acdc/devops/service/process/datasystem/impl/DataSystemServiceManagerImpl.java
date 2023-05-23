package cn.xdf.acdc.devops.service.process.datasystem.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class DataSystemServiceManagerImpl implements DataSystemServiceManager {
    
    private final Map<DataSystemType, DataSystemMetadataService> dataSystemTypeToMetadataServices = new HashMap<>();
    
    private final Map<DataSystemType, DataSystemSourceConnectorService> dataSystemTypeToSourceConnectorServices = new HashMap<>();
    
    private final Map<DataSystemType, DataSystemSinkConnectorService> dataSystemTypeToSinkConnectorServices = new HashMap<>();
    
    /**
     * Init data system services.
     *
     * <p>
     * If there are multiple implements for one data system type, the smallest index one will be used.
     * </p>
     *
     * @param dataSystemMetadataServices data system meta service
     * @param dataSystemSourceConnectorServices data system source connector service
     * @param dataSystemSinkConnectorServices data system sink connector service
     */
    @Autowired
    public void initDataSystemServices(
            final List<DataSystemMetadataService> dataSystemMetadataServices,
            final List<DataSystemSourceConnectorService> dataSystemSourceConnectorServices,
            final List<DataSystemSinkConnectorService> dataSystemSinkConnectorServices) {
        dataSystemMetadataServices.forEach(each -> {
            dataSystemTypeToMetadataServices.putIfAbsent(each.getDataSystemType(), each);
        });
        
        dataSystemSourceConnectorServices.forEach(each -> {
            dataSystemTypeToSourceConnectorServices.putIfAbsent(each.getDataSystemType(), each);
        });
        
        dataSystemSinkConnectorServices.forEach(each -> {
            dataSystemTypeToSinkConnectorServices.putIfAbsent(each.getDataSystemType(), each);
        });
    }
    
    @Override
    public DataSystemMetadataService getDataSystemMetadataService(final DataSystemType dataSystemType) {
        if (dataSystemTypeToMetadataServices.containsKey(dataSystemType)) {
            return dataSystemTypeToMetadataServices.get(dataSystemType);
        }
        throw new ServerErrorException(String.format("can not find a data system metadata service of data system type %s", dataSystemType));
    }
    
    @Override
    public DataSystemSourceConnectorService getDataSystemSourceConnectorService(final DataSystemType dataSystemType) {
        if (dataSystemTypeToSourceConnectorServices.containsKey(dataSystemType)) {
            return dataSystemTypeToSourceConnectorServices.get(dataSystemType);
        }
        throw new ServerErrorException(String.format("can not find a data system source connector service of data system type %s", dataSystemType));
    }
    
    @Override
    public DataSystemSinkConnectorService getDataSystemSinkConnectorService(final DataSystemType dataSystemType) {
        if (dataSystemTypeToSinkConnectorServices.containsKey(dataSystemType)) {
            return dataSystemTypeToSinkConnectorServices.get(dataSystemType);
        }
        throw new ServerErrorException(String.format("can not find a data system sink connector service of data system type %s", dataSystemType));
    }
}
