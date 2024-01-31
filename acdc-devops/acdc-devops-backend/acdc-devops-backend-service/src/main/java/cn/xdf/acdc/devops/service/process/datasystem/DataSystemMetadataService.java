package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;

public interface DataSystemMetadataService extends DataSystemService {
    
    /**
     * Get data system resource definition.
     *
     * @return data system resource definition
     */
    DataSystemResourceDefinition getDataSystemResourceDefinition();
    
    /**
     * Get data collection definition.
     *
     * @param dataCollectionId target data collection id
     * @return data collection definition
     */
    DataCollectionDefinition getDataCollectionDefinition(Long dataCollectionId);
    
    /**
     * Create data collection by data definition.
     *
     * @param parentId parent id
     * @param dataCollectionName data collection name
     * @param dataCollectionDefinition collection definition
     * @return created data collection resource
     */
    DataSystemResourceDTO createDataCollectionByDataDefinition(Long parentId, String dataCollectionName, DataCollectionDefinition dataCollectionDefinition);
    
    /**
     * Check permission and configuration of a data system cluster.
     * <p>
     * eg: check if a mysql instance allow given user to read binlog.
     * </p>
     *
     * @param rootDataSystemResourceId root data system resource id
     */
    void checkDataSystem(Long rootDataSystemResourceId);
    
    /**
     * Check permission and configuration of a data system resource.
     * <p>
     * The resource may have different type, implements must distinguish each type and determine what to do.
     * </p>
     * <p>
     * eg: check if a mysql instance allow given user to read binlog when saving data source instance.
     * </p>
     *
     * @param dataSystemResourceDetail resource to check
     */
    void checkDataSystem(DataSystemResourceDetailDTO dataSystemResourceDetail);
    
    /**
     * Refresh dynamic data system resource for a data system cluster. eg: mysql database and tables.
     *
     * @param rootDataSystemResourceId root data system resource id
     */
    void refreshDynamicDataSystemResource(Long rootDataSystemResourceId);
}
