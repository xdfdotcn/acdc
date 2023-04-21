package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("unused")
@Repository
public interface ConnectionRepository extends JpaRepository<ConnectionDO, Long>, JpaSpecificationExecutor<ConnectionDO> {

    /**
     * Paged query with specific condition.
     *
     * @param query query object
     * @return query paged result
     */
    default Page<ConnectionDO> pagedQuery(final ConnectionQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }

    /**
     * Query with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default List<ConnectionDO> query(ConnectionQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Convert a query object to specification.
     *
     * @param connectionQuery query object
     * @return specification
     */
    default Specification<ConnectionDO> specificationOf(final ConnectionQuery connectionQuery) {
        Preconditions.checkNotNull(connectionQuery);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (!CollectionUtils.isEmpty(connectionQuery.getConnectionIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : connectionQuery.getConnectionIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (!Strings.isNullOrEmpty(connectionQuery.getSinkDataCollectionName())) {
                predicates.add(cb.like(root.get("sinkDataCollection").get("name"), QueryUtil.like("%", connectionQuery.getSinkDataCollectionName(), "%")));
            }

            if (!Strings.isNullOrEmpty(connectionQuery.getSourceDataCollectionName())) {
                predicates.add(cb.like(root.get("sourceDataCollection").get("name"), QueryUtil.like("%", connectionQuery.getSourceDataCollectionName(), "%")));
            }

            if (!CollectionUtils.isEmpty(connectionQuery.getSinkDataCollectionIds())) {
                CriteriaBuilder.In in = cb.in(root.get("sinkDataCollection").get("id"));
                for (Long id : connectionQuery.getSinkDataCollectionIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (Objects.nonNull(connectionQuery.getRequisitionState())) {
                predicates.add(cb.equal(root.get("requisitionState"), connectionQuery.getRequisitionState()));
            }

            if (Objects.nonNull(connectionQuery.getSourceConnectorId())) {
                predicates.add(cb.equal(root.get("sourceConnector").get("id"), connectionQuery.getSourceConnectorId()));
            }

            if (Objects.nonNull(connectionQuery.getSourceDataCollectionId())) {
                predicates.add(cb.equal(root.get("sourceDataCollection").get("id"), connectionQuery.getSourceDataCollectionId()));
            }

            if (Objects.nonNull(connectionQuery.getSinkConnectorId())) {
                predicates.add(cb.equal(root.get("sinkConnector").get("id"), connectionQuery.getSinkConnectorId()));
            }

            if (Objects.nonNull(connectionQuery.getSinkDataSystemType())) {
                predicates.add(cb.equal(root.get("sinkDataCollection").get("dataSystemType"), connectionQuery.getSinkDataSystemType()));
            }

            if (Objects.nonNull(connectionQuery.getActualState())) {
                predicates.add(cb.equal(root.get("actualState"), connectionQuery.getActualState()));
            }
            if (Objects.nonNull(connectionQuery.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), connectionQuery.getDeleted()));
            }

            if (!Strings.isNullOrEmpty(connectionQuery.getDomainAccount())) {
                Join<ConnectionDO, ProjectDO> sinkProjectJoin = root.join("sinkProject", JoinType.INNER);
                Join<ProjectDO, UserDO> userJoin = sinkProjectJoin.join("users", JoinType.INNER);

                predicates.add(cb.equal(userJoin.get("domainAccount"), connectionQuery.getDomainAccount()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
