package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import com.google.common.base.Preconditions;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("unused")
@Repository
public interface ConnectionRequisitionRepository extends JpaRepository<ConnectionRequisitionDO, Long>,
        JpaSpecificationExecutor<ConnectionRequisitionDO> {
    
    /**
     * Get a connection requisition by third party id.
     *
     * @param thirdPartyId third party id
     * @return optional of connection requisition
     */
    Optional<ConnectionRequisitionDO> findByThirdPartyId(String thirdPartyId);
    
    /**
     * Convert a query object to specification.
     *
     * @param query query object
     * @return specification
     */
    default Specification specificationOf(final ConnectionRequisitionQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(query.getConnectionId())) {
                Join<ConnectionRequisitionDO, ConnectionRequisitionConnectionMappingDO> mappingJoin = root.join("connectionRequisitionConnectionMappings", JoinType.INNER);
                predicates.add(cb.equal(mappingJoin.get("connection").get("id"), query.getConnectionId()));
            }
            
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
    
    /**
     * Query with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default List<ConnectionRequisitionDO> query(ConnectionRequisitionQuery query) {
        return findAll(specificationOf(query));
    }
    
    /**
     * Paged query with specific condition.
     *
     * @param query query object
     * @return query paged result
     */
    default Page<ConnectionRequisitionDO> pagedQuery(final ConnectionRequisitionQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
}
