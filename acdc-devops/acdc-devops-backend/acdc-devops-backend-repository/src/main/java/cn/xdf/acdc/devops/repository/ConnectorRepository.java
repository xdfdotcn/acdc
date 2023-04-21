package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Spring Data SQL repository for the Connector entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorRepository extends JpaRepository<ConnectorDO, Long>, JpaSpecificationExecutor<ConnectorDO> {

    /**
     * Find connector by data system resource id.
     *
     * @param dataSystemResourceId data system resouce id
     * @return optional of connector dto
     */
    Optional<ConnectorDO> findByDataSystemResourceId(Long dataSystemResourceId);

    /**
     * Dynamic condition.
     *
     * @param query query
     * @return condition
     */
    default Specification specificationOf(final ConnectorQuery query) {
        Preconditions.checkNotNull(query);

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StringUtils.isNotBlank(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            if (Objects.nonNull(query.getConnectCluster()) && Objects.nonNull(query.getConnectCluster().getId())) {
                predicates.add(cb.equal(root.get("connectCluster"), query.getConnectCluster()));
            }

            if (Objects.nonNull(query.getActualState())) {
                predicates.add(cb.equal(root.get("actualState"), query.getActualState()));
            }

            if (Objects.nonNull(query.getDesiredState())) {
                predicates.add(cb.equal(root.get("desiredState"), query.getDesiredState()));
            }

            if (Objects.nonNull(query.getBeginUpdateTime())) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("updateTime"), query.getBeginUpdateTime()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Query all entity with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default List<ConnectorDO> query(ConnectorQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Paged query with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default Page<ConnectorDO> pagedQuery(final ConnectorQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
}
