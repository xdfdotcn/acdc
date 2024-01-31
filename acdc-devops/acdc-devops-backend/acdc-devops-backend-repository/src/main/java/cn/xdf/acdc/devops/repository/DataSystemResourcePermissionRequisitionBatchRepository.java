package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionBatchDO;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionBatchQuery;
import com.google.common.base.Preconditions;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface DataSystemResourcePermissionRequisitionBatchRepository extends JpaRepository<DataSystemResourcePermissionRequisitionBatchDO, Long>,
        JpaSpecificationExecutor<DataSystemResourcePermissionRequisitionBatchDO> {
    
    /**
     * Query with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default List<DataSystemResourcePermissionRequisitionBatchDO> query(DataSystemResourcePermissionRequisitionBatchQuery query) {
        return findAll(specificationOf(query));
    }
    
    /**
     * Convert a query object to specification.
     *
     * @param query query object
     * @return specification
     */
    default Specification<DataSystemResourcePermissionRequisitionBatchDO> specificationOf(DataSystemResourcePermissionRequisitionBatchQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(query.getBeginUpdateTime())) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("updateTime"), query.getBeginUpdateTime()));
            }
            
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
