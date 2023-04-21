package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataSystemSourceConnectorService extends DataSystemService {

    /**
     * Verify resource.
     *
     * @param dataCollectionId resource id for sink data collection
     */
    void verifyDataSystemMetadata(Long dataCollectionId);

    /**
     * Execute before source connector create.
     *
     * @param dataCollectionId data system resource id for source data collection
     */
    void beforeConnectorCreation(Long dataCollectionId);

    /**
     * Execute after source connector create.
     *
     * @param dataCollectionId data system resource id for source data collection
     */
    void afterConnectorCreation(Long dataCollectionId);

    /**
     * Generate source connector name for target source data collection.
     *
     * @param dataCollectionId data system resource id for source data collection
     * @return connector name
     */
    String generateConnectorName(Long dataCollectionId);

    /**
     * Generate kafka topic name for target source data collection.
     *
     * @param dataCollectionId data system resource id
     * @return topic name
     */
    String generateKafkaTopicName(Long dataCollectionId);

    /**
     * Generate custom source connector configuration (without default configuration) for target source data collections.
     *
     * <p>For relational databases like mysql or tidb,
     * resource is table and param is table ids and the tables belong to one database.
     *
     * @param dataCollectionIds source data collection resource ids
     * @return configurations for source connector
     */
    Map<String, String> generateConnectorCustomConfiguration(List<Long> dataCollectionIds);

    /**
     * Get default configurations for source connector.
     *
     * @return default configurations
     */
    Map<String, String> getConnectorDefaultConfiguration();

    /**
     * Get immutable configuration names.
     *
     * @return sensitive configuration names
     */
    Set<String> getImmutableConfigurationNames();

    /**
     * Get sensitive configuration names.
     *
     * @return sensitive configuration names
     */
    Set<String> getSensitiveConfigurationNames();

    /**
     * Get data system resource type of this data system connector instance.
     *
     * @return data system resource type
     */
    DataSystemResourceType getConnectorDataSystemResourceType();

    /**
     * Get connector detail class DTO.
     *
     * @return connector class detail DTO
     */
    ConnectorClassDetailDTO getConnectorClass();
}
