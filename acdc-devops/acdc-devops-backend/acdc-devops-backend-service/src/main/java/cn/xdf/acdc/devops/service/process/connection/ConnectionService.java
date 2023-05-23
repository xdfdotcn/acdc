package cn.xdf.acdc.devops.service.process.connection;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import org.springframework.data.domain.Page;

import java.util.List;
import java.util.Set;

public interface ConnectionService {
    
    /**
     * Query list.
     *
     * @param query query
     * @return List
     */
    List<ConnectionDTO> query(ConnectionQuery query);
    
    /**
     * Detail paged query connection.
     *
     * @param query query
     * @return Page
     */
    Page<ConnectionDTO> pagedQuery(ConnectionQuery query);
    
    /**
     * Get connection.
     *
     * @param id connection id
     * @return connection
     */
    ConnectionDTO getById(Long id);
    
    /**
     * Get connections.
     *
     * @param connectionIds connection id collection
     * @return connection list
     */
    List<ConnectionDTO> getByIds(Set<Long> connectionIds);
    
    /**
     * Get connection detail.
     *
     * @param id id
     * @return connection detail
     */
    ConnectionDetailDTO getDetailById(Long id);
    
    /**
     * Delete connection.
     *
     * @param id id
     */
    void deleteById(Long id);
    
    /**
     * Bulk delete connection.
     *
     * @param ids ids
     */
    void deleteByIds(Set<Long> ids);
    
    /**
     * Bulk create connection.
     *
     * @param connections connections
     * @param domainAccount domainAccount
     * @return ConnectionDTO list
     */
    List<ConnectionDetailDTO> batchCreate(List<ConnectionDetailDTO> connections, String domainAccount);
    
    /**
     * Bulk edit connections.
     *
     * @param connections connections
     * @return modified connections
     */
    List<ConnectionDTO> batchUpdate(List<ConnectionDetailDTO> connections);
    
    /**
     * Update connection actual state.
     *
     * @param connectionId connection ID
     * @param state state
     */
    void updateActualState(Long connectionId, ConnectionState state);
    
    /**
     * Update connection desired state.
     *
     * @param connectionId connector ID
     * @param state state
     */
    void updateDesiredState(Long connectionId, ConnectionState state);
    
    /**
     * Start connection.
     *
     * @param connectionId connection id
     */
    void start(Long connectionId);
    
    /**
     * Stop connection.
     *
     * @param connectionId connection id
     */
    
    void stop(Long connectionId);
    
    /**
     * Get connection actual state.
     *
     * @param connectionId connector ID
     * @return state
     */
    ConnectionState getActualState(Long connectionId);
    
    /**
     * Bulk edit connection requisition state.
     *
     * @param connectionQuery connectionIds
     * @param requisitionState requisitionState
     */
    void updateConnectionRequisitionStateByQuery(ConnectionQuery connectionQuery, RequisitionState requisitionState);
    
    /**
     * Apply connection to connector.
     *
     * @param connectionId connection id
     * @return saved connection
     */
    ConnectionDTO applyConnectionToConnector(Long connectionId);
    
    /**
     * Apply connection configuration to connector.
     *
     * @param connectionId connection id
     */
    void applyConnectionConfigurationToConnector(Long connectionId);
}
