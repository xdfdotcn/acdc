package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionColumnConfigurationQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.persistence.criteria.Predicate;

import org.springframework.data.jpa.domain.Specification;

public interface ConnectionColumnConfigurationService {

    /**
     * Query list.
     *
     * @param query query
     * @return List
     */
    List<ConnectionColumnConfigurationDO> query(ConnectionColumnConfigurationQuery query);

    /**
     * Save connectionColumnConfiguration.
     *
     * @param connectionColumnConfiguration connectionColumnConfiguration
     * @return saved connectionColumnConfiguration
     */
    ConnectionColumnConfigurationDO save(ConnectionColumnConfigurationDO connectionColumnConfiguration);

    /**
     * Bulk save .
     *
     * @param connectionColumnConfigurations connectionColumnConfigurations
     * @return saved connectionColumnConfigurations
     */
    List<ConnectionColumnConfigurationDO> saveAll(List<ConnectionColumnConfigurationDO> connectionColumnConfigurations);

    /**
     * Find connectionColumnConfiguration by ID.
     *
     * @param id id
     * @return connectionColumnConfiguration
     */
    Optional<ConnectionColumnConfigurationDO> findById(Long id);


    /**
     * Dynamic conditions.
     *
     * @param connectionQuery connectionQuery
     * @return Specification
     */
    static Specification specificationOf(final ConnectionColumnConfigurationQuery connectionQuery) {

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(connectionQuery.getConnectionId())) {
                predicates.add(cb.equal(root.get("connection"),
                        ConnectionDO.builder().id(connectionQuery.getConnectionId()).build()));
            }
            if (Objects.nonNull(connectionQuery.getVersion())) {
                predicates.add(cb.equal(root.get("connectionVersion"), connectionQuery.getVersion()));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
