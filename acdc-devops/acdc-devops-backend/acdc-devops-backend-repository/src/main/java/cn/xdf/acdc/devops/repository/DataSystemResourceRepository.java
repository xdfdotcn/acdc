package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourceQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Spring Data SQL repository for the ConnectorEvent entity.
 */
@Repository
public interface DataSystemResourceRepository extends JpaRepository<DataSystemResourceDO, Long>, JpaSpecificationExecutor<DataSystemResourceDO> {
    
    /**
     * Find all resource that has not been logically deleted by parent resource id and resource type.
     *
     * @param parentResourceId parent resource id
     * @param resourceType resource type
     * @return data system resources
     */
    List<DataSystemResourceDO> findByDeletedFalseAndParentResourceIdAndResourceType(Long parentResourceId, DataSystemResourceType resourceType);
    
    /**
     * Find all data system resource that has not been logically deleted and parent resource id is null.
     *
     * @return data system resources
     */
    List<DataSystemResourceDO> findByDeletedFalseAndParentResourceIdIsNull();
    
    /**
     * Convert a query object to specification.
     *
     * @param query query object
     * @return specification
     */
    default Specification specificationOf(final DataSystemResourceQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            
            if (Objects.nonNull(query.getParentResourceId())) {
                predicates.add(cb.equal(root.get("parentResource").get("id"), query.getParentResourceId()));
            }
            
            if (!Strings.isNullOrEmpty(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }
            
            if (Objects.nonNull(query.getResourceTypes())) {
                predicates.add(root.get("resourceType").in(query.getResourceTypes()));
            }
            
            if (!CollectionUtils.isEmpty(query.getResourceConfigurations())) {
                Join<DataSystemResourceDO, DataSystemResourceConfigurationDO> joinDataSystemResourceConfiguration = root.join("dataSystemResourceConfigurations", JoinType.INNER);
                query.getResourceConfigurations().forEach((name, value) -> {
                    predicates.add(cb.equal(joinDataSystemResourceConfiguration.get("name"), name));
                    predicates.add(cb.equal(joinDataSystemResourceConfiguration.get("value"), value));
                });
            }
            
            if (!CollectionUtils.isEmpty(query.getProjectIds())) {
                Join<DataSystemResourceDO, ProjectDO> joinProject = root.join("projects", JoinType.INNER);
                predicates.add(joinProject.get("id").in(query.getProjectIds()));
            }
            
            if (!Strings.isNullOrEmpty(query.getMemberDomainAccount())) {
                Join<ProjectDO, UserDO> userJoin = root.join("projects", JoinType.INNER).join("users", JoinType.INNER);
                predicates.add(cb.equal(userJoin.get("domainAccount"), query.getMemberDomainAccount()));
            }
            
            if (Objects.nonNull(query.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), query.getDeleted()));
            }
            criteriaQuery.distinct(true);
            
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
    
    /**
     * Query with specific condition.
     *
     * @param query query object
     * @return query result
     */
    default List<DataSystemResourceDO> query(DataSystemResourceQuery query) {
        return findAll(specificationOf(query));
    }
    
    /**
     * Paged query with specific condition.
     *
     * @param query query object
     * @return query paged result
     */
    default Page<DataSystemResourceDO> pagedQuery(final DataSystemResourceQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
}
