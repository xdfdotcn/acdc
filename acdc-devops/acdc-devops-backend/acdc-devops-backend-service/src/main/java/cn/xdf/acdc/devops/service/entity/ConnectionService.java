package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionInfoQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Connection service.
 */
public interface ConnectionService {

    /**
     * Query list.
     *
     * @param query query
     * @return List
     */
    List<ConnectionDO> query(ConnectionQuery query);

    /**
     * Query page.
     *
     * @param query    query
     * @param pageable pageable
     * @return List
     */

    Page<ConnectionDO> pagingQuery(ConnectionQuery query, Pageable pageable);


    /**
     * Detail Paging Query.
     *
     * @param query              query
     * @param sinkDataSystemType sinkDataSystemType
     * @param pageable           pageable
     * @param domainAccount      domainAccount
     * @return List
     */

    Page<Map<String, Object>> detailPagingQuery(DataSystemType sinkDataSystemType, ConnectionInfoQuery query, Pageable pageable, String domainAccount);


    /**
     * Save connection.
     *
     * @param connection connection
     * @return saved connection
     */
    ConnectionDO save(ConnectionDO connection);

    /**
     * Bulk save.
     *
     * @param connections connections
     * @return saved connection
     */
    List<ConnectionDO> saveAll(List<ConnectionDO> connections);

    /**
     * Find connection by ID.
     *
     * @param id id
     * @return connection
     */
    Optional<ConnectionDO> findById(Long id);

    /**
     * Find connection by ids.
     *
     * @param ids ids
     * @return connection
     */
    List<ConnectionDO> findAllById(Set<Long> ids);

    /**
     * Exists connection.
     *
     * @param datasetIds datasetIds
     * @return boolean
     */
    boolean existsEachInSinkDatasetIds(List<Long> datasetIds);

    /**
     * Get newest column config.
     *
     * @param connectionId connectionId
     * @return List
     */
    List<ConnectionColumnConfigurationDO> findNewestColumnConfig(Long connectionId);

    /**
     * Update actual state.
     *
     * @param connectionId connectionId
     * @param state        state
     */
    void updateActualState(Long connectionId, ConnectionState state);

    /**
     * Update desired state.
     *
     * @param connectionId connectionId
     * @param state        state
     */
    void updateDesiredState(Long connectionId, ConnectionState state);

    /**
     * Dynamic conditions.
     *
     * @param connectionQuery connectionQuery
     * @return Specification
     */
    static Specification specificationOf(final ConnectionQuery connectionQuery) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.nonNull(connectionQuery.getConnectionId())) {
                predicates.add(cb.equal(root.get("id"), connectionQuery.getConnectionId()));
            }

            if (Objects.nonNull(connectionQuery.getBeginUpdateTime())) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("updateTime"), connectionQuery.getBeginUpdateTime()));
            }

            if (Objects.nonNull(connectionQuery.getRequisitionState())) {
                predicates.add(cb.equal(root.get("requisitionState"), connectionQuery.getRequisitionState()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
