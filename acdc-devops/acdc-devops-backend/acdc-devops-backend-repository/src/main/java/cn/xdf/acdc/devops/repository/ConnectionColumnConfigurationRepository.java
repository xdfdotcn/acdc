package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionColumnConfigurationQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.persistence.criteria.Predicate;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

// CHECKSTYLE:OFF
public interface ConnectionColumnConfigurationRepository extends JpaRepository<ConnectionColumnConfigurationDO, Long>, JpaSpecificationExecutor<ConnectionColumnConfigurationDO> {

    default List<ConnectionColumnConfigurationDO> query(ConnectionColumnConfigurationQuery query) {
        return findAll(specificationOf(query));
    }

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
                predicates.add(cb.equal(root.get("connectionId"), connectionQuery.getConnectionId()));
            }
            if (Objects.nonNull(connectionQuery.getVersion())) {
                predicates.add(cb.equal(root.get("connectionVersion"), connectionQuery.getVersion()));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
