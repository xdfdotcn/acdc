package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionQuery;
import com.google.common.base.Preconditions;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

// CHECKSTYLE:OFF
public interface ConnectionRequisitionRepository extends JpaRepository<ConnectionRequisitionDO, Long>,
        JpaSpecificationExecutor<ConnectionRequisitionDO> {

    Optional<ConnectionRequisitionDO> findByThirdPartyId(String thirdPartyId);

    default List<ConnectionRequisitionDO> query(ConnectionRequisitionQuery query) {
        return findAll(specificationOf(query));
    }

    default Page<ConnectionRequisitionDO> pagingQuery(final ConnectionRequisitionQuery query, final Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }

    static Specification specificationOf(final ConnectionRequisitionQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (!CollectionUtils.isEmpty(query.getConnectionRequisitionIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : query.getConnectionRequisitionIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
