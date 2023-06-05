package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

public interface DataSystemServiceManager {
    
    /**
     * Get data system metadata service of a data system.
     *
     * @param dataSystemType data system type
     * @return data system meta service
     */
    DataSystemMetadataService getDataSystemMetadataService(DataSystemType dataSystemType);
    
    /**
     * Get data system source connector service of a data system.
     *
     * @param dataSystemType data system type
     * @return data system source connector service
     */
    DataSystemSourceConnectorService getDataSystemSourceConnectorService(DataSystemType dataSystemType);
    
    /**
     * Get data system sink connector service of a data system.
     *
     * @param dataSystemType data system type
     * @return data system sink connector service
     */
    DataSystemSinkConnectorService getDataSystemSinkConnectorService(DataSystemType dataSystemType);
}
