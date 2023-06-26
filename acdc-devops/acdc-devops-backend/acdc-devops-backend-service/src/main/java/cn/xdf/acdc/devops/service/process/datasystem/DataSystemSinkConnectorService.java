package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;

import java.util.Map;
import java.util.Set;

public interface DataSystemSinkConnectorService extends DataSystemService {
    
    /**
     * Verify resource.
     *
     * @param dataCollectionId resource id for data collection
     */
    void verifyDataSystemMetadata(Long dataCollectionId);
    
    /**
     * Execute before connector create.
     *
     * @param dataCollectionId resource id for data collection
     */
    void beforeConnectorCreation(Long dataCollectionId);
    
    /**
     * Get the connector default configuration.
     *
     * @return default configurations for connector
     */
    Map<String, String> getConnectorDefaultConfiguration();
    
    /**
     * Generate the connector custom configuration.
     *
     * @param connectionId connection id
     * @return custom configuration
     */
    Map<String, String> generateConnectorCustomConfiguration(Long connectionId);
    
    /**
     * Generate connector name for target data collection.
     *
     * @param connectionId connection id
     * @return connector name
     */
    String generateConnectorName(Long connectionId);
    
    /**
     * Get  connector specific configuration definitions.
     *
     * @return configuration definitions
     */
    Set<ConfigurationDefinition<?>> getConnectorSpecificConfigurationDefinitions();
    
    /**
     * Get sensitive configuration keys.
     *
     * @return sensitive configuration keys
     */
    Set<String> getSensitiveConfigurationNames();
    
    /**
     * Get connector detail class DTO.
     *
     * @return connector class detail DTO
     */
    ConnectorClassDetailDTO getConnectorClass();
}
