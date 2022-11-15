package cn.xdf.acdc.devops.service.process.connection;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionEditDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionInfoQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import org.springframework.data.domain.Page;

import java.util.List;
import java.util.Set;

public interface ConnectionProcessService {

    /**
     * Query list.
     *
     * @param query query
     * @return List
     */
    List<ConnectionDetailDTO> query(ConnectionQuery query);


    /**
     * Detail query.
     *
     * @param query query
     * @return ConnectionDetailDTO list
     */
    List<ConnectionDetailDTO> detailQuery(ConnectionQuery query);

    /**
     * Paging query connection.
     *
     * @param query query
     * @return Page
     */
    Page<ConnectionDetailDTO> pagingQuery(ConnectionQuery query);


    /**
     * Get connection detail.
     *
     * @param id id
     * @return connection detail
     */
    ConnectionDetailDTO getConnectionDetail(Long id);

    /**
     * Delete connection.
     *
     * @param id id
     */
    void deleteConnection(Long id);

    /**
     * Bulk delete connection.
     *
     * @param ids ids
     */
    void bulkDeleteConnection(Set<Long> ids);

    /**
     * Detail Paging query connection.
     *
     * @param dataSystemType dataSystemType
     * @param query          query
     * @param domainAccount  domainAccount
     * @return Page
     */
    Page<ConnectionInfoDTO> detailPagingQuery(DataSystemType dataSystemType, ConnectionInfoQuery query, String domainAccount);


    /**
     * Apply connection to connector.
     *
     * @param connection connection
     * @return saved connection
     */
    ConnectionDetailDTO applyConnectionToConnector(ConnectionDetailDTO connection);

    /**
     * Flush connector's config.
     *
     * @param connection connection
     */
    void flushConnectionConfigToConnector(ConnectionDetailDTO connection);

    /**
     * Bulk create connection.
     *
     * @param connections   connections
     * @param domainAccount domainAccount
     * @return ConnectionDTO list
     */
    List<ConnectionDTO> bulkCreateConnection(List<ConnectionDetailDTO> connections, String domainAccount);

    /**
     * Bulk edit connections.
     *
     * @param connections connections
     */
    void bulkEditConnection(List<ConnectionEditDTO> connections);

    /**
     * Edit connection actual state.
     *
     * @param connectionId connection ID
     * @param state        state
     */
    void editActualState(Long connectionId, ConnectionState state);

    /**
     * Edit connection desired state.
     *
     * @param connectionId connector ID
     * @param state        state
     */
    void editDesiredState(Long connectionId, ConnectionState state);

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
     * @param connectionQuery  connectionIds
     * @param requisitionState requisitionState
     */
    void bulkEditConnectionRequisitionStateByQuery(ConnectionQuery connectionQuery, RequisitionState requisitionState);
}
