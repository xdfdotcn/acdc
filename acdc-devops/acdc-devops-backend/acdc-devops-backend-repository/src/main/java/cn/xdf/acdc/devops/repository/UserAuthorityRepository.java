package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.UserAuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserAuthorityQuery;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;

import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.util.CollectionUtils;

// CHECKSTYLE:OFF
public interface UserAuthorityRepository extends JpaRepository<UserAuthorityDO, String>, JpaSpecificationExecutor<UserAuthorityDO> {

    default List<UserAuthorityDO> queryAll(UserAuthorityQuery query) {
        return findAll(specificationOf(query));
    }

    static Specification specificationOf(final UserAuthorityQuery query) {
        Preconditions.checkNotNull(query);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (!CollectionUtils.isEmpty(query.getAuthorityRoleTypes())) {
                CriteriaBuilder.In in = cb.in(root.get("authorityName"));
                for (AuthorityRoleType role : query.getAuthorityRoleTypes()) {
                    in.value(role.name());
                }
                predicates.add(in);
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Delete role by userId.
     * @param userId
     */
    void deleteRoleByUserId(Long userId);
}
