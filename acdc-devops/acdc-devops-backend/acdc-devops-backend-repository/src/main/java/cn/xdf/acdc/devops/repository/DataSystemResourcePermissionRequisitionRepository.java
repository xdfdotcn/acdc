package cn.xdf.acdc.devops.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;

import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import com.google.common.base.Preconditions;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionQuery;

public interface DataSystemResourcePermissionRequisitionRepository extends JpaRepository<DataSystemResourcePermissionRequisitionDO, Long>,
        JpaSpecificationExecutor<DataSystemResourcePermissionRequisitionDO> {
    /**
     * Query with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default List<DataSystemResourcePermissionRequisitionDO> query(DataSystemResourcePermissionRequisitionQuery query) {
        return findAll(specificationOf(query));
    }
    
    /**
     * Convert a query object to specification.
     *
     * @param query query object
     * @return specification
     */
    default Specification<DataSystemResourcePermissionRequisitionDO> specificationOf(DataSystemResourcePermissionRequisitionQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(query.getBeginUpdateTime())) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("updateTime"), query.getBeginUpdateTime()));
            }
            
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }
    
    /**
     * Find  by source project id and data system resource id.
     *
     * @param sourceProjectId source project id
     * @param sinkProjectId sink project id
     * @param dataSystemResourceId data system resource id
     * @return list
     */
    default List<DataSystemResourcePermissionRequisitionDO> findBySourceProjectIdAndDataSystemResourceId(Long sourceProjectId, Long sinkProjectId, Long dataSystemResourceId) {
        Specification<DataSystemResourcePermissionRequisitionDO> specification = (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            
            Join<DataSystemResourcePermissionRequisitionDO, DataSystemResourceDO> dataSystemResourceJoin =
                    root.join("dataSystemResources", JoinType.INNER);
            
            predicates.add(cb.equal(root.get("sourceProject").get("id"), sourceProjectId));
            predicates.add(cb.equal(root.get("sinkProject").get("id"), sinkProjectId));
            predicates.add(cb.equal(dataSystemResourceJoin.get("id"), dataSystemResourceId));
            return cb.and(predicates.toArray(new Predicate[0]));
        };
        
        return findAll(specification);
    }
    
    /**
     * Get a connection requisition by third party id.
     *
     * @param thirdPartyId third party id
     * @return optional of connection requisition
     */
    Optional<DataSystemResourcePermissionRequisitionDO> findByThirdPartyId(String thirdPartyId);
}

