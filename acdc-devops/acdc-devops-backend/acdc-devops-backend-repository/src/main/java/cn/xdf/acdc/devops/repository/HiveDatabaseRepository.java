package cn.xdf.acdc.devops.repository;

// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.query.HiveDatabaseQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.persistence.criteria.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the HiveDatabase entity.
 */
@SuppressWarnings("unused")
@Repository
public interface HiveDatabaseRepository extends JpaRepository<HiveDatabaseDO, Long>, JpaSpecificationExecutor {

    /**
     * Dynamic condition.
     *
     * @param query query
     * @return condition
     */
    static Specification specificationOf(final HiveDatabaseQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StringUtils.isNotBlank(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            if (Objects.nonNull(query.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), query.getDeleted()));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    default List<HiveDatabaseDO> query(HiveDatabaseQuery query) {
        return findAll(specificationOf(query));
    }

    default Page<HiveDatabaseDO> pagingQuery(final HiveDatabaseQuery query, final Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }
}
