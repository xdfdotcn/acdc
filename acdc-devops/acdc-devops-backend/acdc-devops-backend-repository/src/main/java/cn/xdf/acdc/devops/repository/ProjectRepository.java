package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
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

/**
 * Repository for the Project entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ProjectRepository extends JpaRepository<ProjectDO, Long>, JpaSpecificationExecutor<ProjectDO> {

    /**
     * Find projects by source type.
     *
     * @param source type
     * @return projects
     */
    List<ProjectDO> findBySource(MetadataSourceType source);

    /**
     * Dynamic condition.
     *
     * @param query query
     * @return condition
     */
    default Specification specificationOf(final ProjectQuery query) {
        Preconditions.checkNotNull(query);

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StringUtils.isNotBlank(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            if (!CollectionUtils.isEmpty(query.getProjectIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : query.getProjectIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (!Strings.isNullOrEmpty(query.getOwnerDomainAccount())) {
                Join<ProjectDO, UserDO> userJoin = root.join("owner", JoinType.INNER);
                predicates.add(cb.equal(userJoin.get("domainAccount"), query.getOwnerDomainAccount()));
            }

            if (!Strings.isNullOrEmpty(query.getMemberDomainAccount())) {
                Join<ProjectDO, UserDO> userJoin = root.join("users", JoinType.INNER);
                predicates.add(cb.equal(userJoin.get("domainAccount"), query.getMemberDomainAccount()));
            }

            if (Objects.nonNull(query.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), query.getDeleted()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Common query.
     *
     * @param query query
     * @return list
     */
    default List<ProjectDO> query(final ProjectQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Paging query.
     *
     * @param query query
     * @return Paging list
     */
    default Page<ProjectDO> pagedQuery(final ProjectQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
}
