package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.query.HiveTableQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Spring Data SQL repository for the HiveTable entity.
 */
@SuppressWarnings("unused")
@Repository
public interface HiveTableRepository extends JpaRepository<HiveTableDO, Long>, JpaSpecificationExecutor {

    /**
     * Query hive table by filter.
     *
     * @param query query filter
     * @return hive tables
     */
    default List<HiveTableDO> queryAll(HiveTableQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Query hive table by filter and page.
     *
     * @param query    query filter
     * @param pageable page
     * @return hive tables
     */
    default Page<HiveTableDO> queryAll(final HiveTableQuery query, final Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }

    /**
     * Convert query object to specification.
     *
     * @param query query filter
     * @return specification
     */
    default Specification specificationOf(final HiveTableQuery query) {
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
}
