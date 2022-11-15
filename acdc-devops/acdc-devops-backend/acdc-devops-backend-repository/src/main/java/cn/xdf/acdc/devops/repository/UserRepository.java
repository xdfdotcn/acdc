package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

// CHECKSTYLE:OFF

/**
 * Spring Data JPA repository for the {@link UserDO} entity.
 */
@Repository
public interface UserRepository extends JpaRepository<UserDO, Long>, JpaSpecificationExecutor {

    String USERS_BY_LOGIN_CACHE = "usersByLogin";

    String USERS_BY_EMAIL_CACHE = "usersByEmail";

    /**
     * 动态条件.
     *
     * @param query query
     * @return 动态条件
     */
    static Specification specificationOf(final UserQuery query) {
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
     * 根据条件查询用户列表.
     *
     * @param specification 自定义条件
     * @return java.util.List
     * @date 2022/8/1 6:04 下午
     */
    @Override
    List<UserDO> findAll(Specification specification);

    /**
     * 查询用户列表.
     *
     * @param query query
     * @return 用户列表
     */
    default List<UserDO> query(UserQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Paging query user.
     *
     * @param query    query
     * @param pageable pageable
     * @return Page
     */
    default Page<UserDO> queryAll(UserQuery query, Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }
}
