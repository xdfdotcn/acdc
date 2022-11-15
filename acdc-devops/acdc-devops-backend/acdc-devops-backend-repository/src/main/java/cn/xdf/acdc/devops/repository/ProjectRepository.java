package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

/**
 * Repository for the Project entity.
 */
@Repository
public interface ProjectRepository extends JpaRepository<ProjectDO, Long>, JpaSpecificationExecutor {

    /**
     * Dynamic condition.
     *
     * @param query query
     * @return condition
     */
    static Specification specificationOf(final ProjectQuery query) {
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

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Common query.
     *
     * @param query query
     * @return list
     */
    default List<ProjectDO> queryAll(final ProjectQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Paging query.
     *
     * @param query query
     * @param pageable pageable
     * @return Paging list
     */
    default Page<ProjectDO> queryAll(final ProjectQuery query, final Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }
}
