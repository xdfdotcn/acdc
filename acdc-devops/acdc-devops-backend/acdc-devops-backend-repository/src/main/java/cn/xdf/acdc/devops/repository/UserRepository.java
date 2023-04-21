package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


/**
 * Spring Data JPA repository for the {@link UserDO} entity.
 */
@Repository
public interface UserRepository extends JpaRepository<UserDO, Long>, JpaSpecificationExecutor<UserDO> {

    String USERS_BY_LOGIN_CACHE = "usersByLogin";

    String USERS_BY_EMAIL_CACHE = "usersByEmail";

    /**
     * Generate dynamic condition.
     *
     * @param query query
     * @return condition
     */
    default Specification specificationOf(final UserQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (!CollectionUtils.isEmpty(query.getUserIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : query.getUserIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (!Strings.isNullOrEmpty(query.getDomainAccount())) {
                predicates.add(cb.like(root.get("domainAccount"), QueryUtil.like("%", query.getDomainAccount(), "%")));
            }

            if (Objects.nonNull(query.getProjectId())) {
                Join<UserDO, ProjectDO> projectRoot = root.join("projects", JoinType.INNER);
                predicates.add(cb.equal(projectRoot.get("id"), query.getProjectId()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * 根据邮箱查询.
     *
     * @param email email
     * @return User
     */
    Optional<UserDO> findOneByEmailIgnoreCase(String email);

    /**
     * 根据域账号查询.
     *
     * @param domainAccount domainAccount
     * @return User
     */
    Optional<UserDO> findOneByDomainAccountIgnoreCase(String domainAccount);

    /**
     * 根据邮箱和角色查询.
     *
     * @param email email
     * @return User
     */
    @EntityGraph(attributePaths = "authorities")
    @Cacheable(cacheNames = USERS_BY_EMAIL_CACHE)
    Optional<UserDO> findOneWithAuthoritiesByEmailIgnoreCase(String email);

    /**
     * Common query.
     *
     * @param query query
     * @return list
     */
    default List<UserDO> query(UserQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Paged query.
     *
     * @param query query
     * @return Paged list
     */
    default Page<UserDO> pagedQuery(final UserQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
}
