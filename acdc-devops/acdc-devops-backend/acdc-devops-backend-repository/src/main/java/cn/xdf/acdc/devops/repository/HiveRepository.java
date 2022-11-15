package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.query.HiveQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
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
 * Spring Data SQL repository for the Hive entity.
 */
@Repository
public interface HiveRepository extends JpaRepository<HiveDO, Long>, JpaSpecificationExecutor {

    /**
     * Query hive by filter.
     *
     * @param query query filter
     * @return hive
     */
    default List<HiveDO> queryAll(final HiveQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Query hive by filter and page.
     *
     * @param query    query filter
     * @param pageable page
     * @return paged hives
     */
    default Page<HiveDO> queryAll(final HiveQuery query, final Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }

    /**
     * Convert query object to specification.
     *
     * @param query query filter
     * @return specification
     */
    default Specification specificationOf(final HiveQuery query) {
        Preconditions.checkNotNull(query);

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (!CollectionUtils.isEmpty(query.getHiveIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : query.getHiveIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (StringUtils.isNotBlank(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
