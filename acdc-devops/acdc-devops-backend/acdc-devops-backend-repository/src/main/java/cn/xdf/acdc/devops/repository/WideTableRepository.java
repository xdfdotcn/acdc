package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDO;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.domain.query.WideTableQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface WideTableRepository extends JpaRepository<WideTableDO, Long>,
        JpaSpecificationExecutor<WideTableDO> {
    
    /**
     * If exist with the given name and project id.
     *
     * @param name wide table name
     * @param projectId project id
     * @return exist or not
     */
    boolean existsByNameAndProjectId(String name, Long projectId);
    
    /**
     * Query with specific condition.
     *
     * @param wideTableQuery query object
     * @return query result
     */
    default List<WideTableDO> query(WideTableQuery wideTableQuery) {
        return findAll(specificationOf(wideTableQuery));
    }
    
    /**
     * Paged query with specific condition.
     *
     * @param query query object
     * @return query paged result
     */
    default Page<WideTableDO> pagedQuery(final WideTableQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
    
    /**
     * Convert a query object to specification.
     *
     * @param wideTableQuery query object
     * @return specification
     */
    default Specification<WideTableDO> specificationOf(WideTableQuery wideTableQuery) {
        Preconditions.checkNotNull(wideTableQuery);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (Objects.nonNull(wideTableQuery.getBeginUpdateTime())) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("updateTime"), wideTableQuery.getBeginUpdateTime()));
            }
            
            if (StringUtils.isNotBlank(wideTableQuery.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", wideTableQuery.getName(), "%")));
            }
            
            if (Objects.nonNull(wideTableQuery.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), wideTableQuery.getDeleted()));
            }
            
            if (Objects.nonNull(wideTableQuery.getDataCollectionId())) {
                Join<WideTableDO, DataSystemResourceDO> dataCollectionJoin = root.join("dataCollection", JoinType.INNER);
                predicates.add(cb.equal(dataCollectionJoin.get("id"), wideTableQuery.getDataCollectionId()));
            }
            
            if (Objects.nonNull(wideTableQuery.getDomainAccount())) {
                predicates.add(cb.equal(root.get("deleted"), wideTableQuery.getDomainAccount()));
            }
            
            if (!Strings.isNullOrEmpty(wideTableQuery.getDomainAccount())) {
                Join<ProjectDO, UserDO> userJoin = root.join("project", JoinType.INNER).join("users", JoinType.INNER);
                predicates.add(cb.equal(userJoin.get("domainAccount"), wideTableQuery.getDomainAccount()));
            }
            
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }
}
