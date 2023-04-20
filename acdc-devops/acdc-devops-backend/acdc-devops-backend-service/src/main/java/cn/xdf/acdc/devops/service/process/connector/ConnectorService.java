package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import org.springframework.data.domain.Page;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ConnectorService {

    /**
     * Create a connector.
     *
     * @param connectorDetailDTO connectorDetailDTO
     * @return created connector detail DTO
     */
    ConnectorDetailDTO create(ConnectorDetailDTO connectorDetailDTO);

    /**
     * Get connector by data system resource id.
     *
     * @param dataSystemResourceId data system resource id
     * @return optional of ConnectorDetailDTO
     */
    Optional<ConnectorDetailDTO> getDetailByDataSystemResourceId(Long dataSystemResourceId);

    /**
     * Update the connector particular configuration with the given configuration.
     *
     * @param connectorId    connector id
     * @param configurations connector configuration
     * @return new configuration
     */
    Map<String, String> updateParticularConfiguration(Long connectorId, Map<String, String> configurations);

    /**
     * Update the connector entire configuration with the given configuration.
     *
     * @param connectorId    connector id
     * @param configurations connector configuration
     * @return new configuration
     */
    Map<String, String> updateEntireConfiguration(Long connectorId, Map<String, String> configurations);

    /**
     * Update connector actual state.
     *
     * @param connectorId connector id
     * @param actualState actual state
     */
    void updateActualState(Long connectorId, ConnectorState actualState);

    /**
     * Query connector.
     *
     * @param query query
     * @return data system resource list
     */
    List<ConnectorDTO> query(ConnectorQuery query);

    /**
     * Query connector detail.
     *
     * @param query query
     * @return data system resource list
     */
    List<ConnectorDetailDTO> queryDetail(ConnectorQuery query);

    /**
     * Query connector detail, All configuration is decrypted.
     *
     * @param query query
     * @return data system resource list
     */
    List<ConnectorDetailDTO> queryDetailWithDecryptConfiguration(ConnectorQuery query);

    /**
     * Paged query connector.
     *
     * @param query query
     * @return data system resource paged list
     */
    Page<ConnectorDTO> pagedQuery(ConnectorQuery query);

    /**
     * Start the connector.
     *
     * @param connectorId connector id
     */
    void start(Long connectorId);

    /**
     * Stop the connector.
     *
     * @param connectorId connector id
     */
    void stop(Long connectorId);

    /**
     * Get connector by id.
     *
     * @param connectorId connector id
     * @return connector detail DTO
     */
    ConnectorDetailDTO getDetailById(Long connectorId);
}
