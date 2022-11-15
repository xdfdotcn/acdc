package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;

/**
 * Spring Data SQL repository for the Rdb entity.
 */
@Repository
public interface RdbRepository extends JpaRepository<RdbDO, Long>, JpaSpecificationExecutor {

    /**
     * Query rdb by filter.
     *
     * @param query query filter
     * @return rdbs
     */
    default List<RdbDO> queryAll(final RdbQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Query rdb by filter and page.
     *
     * @param query    query filter
     * @param pageable page
     * @return paged rdbs
     */
    default Page<RdbDO> queryAll(final RdbQuery query, final Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }

    /**
     * Convert query object to specification.
     *
     * @param query query filter
     * @return specification
     */
    default Specification specificationOf(final RdbQuery query) {
        Preconditions.checkNotNull(query);

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (!CollectionUtils.isEmpty(query.getRdbIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : query.getRdbIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (!Strings.isNullOrEmpty(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            if (!Strings.isNullOrEmpty(query.getRdbType())) {
                predicates.add(cb.equal(root.get("rdbType"), query.getRdbType()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
