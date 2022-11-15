package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionConnectionMappingQuery;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.persistence.criteria.Predicate;

import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

// CHECKSTYLE:OFF
public interface ConnectionRequisitionConnectionMappingRepository
        extends JpaRepository<ConnectionRequisitionConnectionMappingDO, Long>,
        JpaSpecificationExecutor<ConnectionRequisitionConnectionMappingDO> {

    default List<ConnectionRequisitionConnectionMappingDO> query(ConnectionRequisitionConnectionMappingQuery query) {
        return findAll(specificationOf(query));
    }

    static Specification specificationOf(final ConnectionRequisitionConnectionMappingQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(query.getConnectionRequisitionId())) {
                predicates.add(cb.equal(root.get("connectionRequisition")
                        , ConnectionRequisitionDO.builder().id(query.getConnectionRequisitionId()).build()));
            }
            if (Objects.nonNull(query.getConnectionId())) {
                predicates.add(cb.equal(root.get("connection")
                        , ConnectionDO.builder().id(query.getConnectionId()).build())
                );
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
