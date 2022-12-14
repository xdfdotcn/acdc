package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import com.google.common.base.Strings;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public interface UserService {


    /**
     * Create a new user.
     *
     * @param userDetail userDetail
     * @return UserDTO
     */
    UserDTO createUser(UserDetailDTO userDetail);

    /**
     * Update user.
     *
     * @param userDetail userDetail
     * @return UserDTO
     */
    UserDTO updateUser(UserDetailDTO userDetail);

    /**
     * Rest password.
     *
     * @param email       email
     * @param oldPassword oldPassword
     * @param newPassword newPassword
     */
    void resetPassword(String email, String oldPassword, String newPassword);

    /**
     * Rest role.
     *
     * @param email       email
     * @param roleTypeSet roleTypeSet
     */
    void resetRole(String email, Set<AuthorityRoleType> roleTypeSet);

    /**
     * Paging query user.
     *
     * @param userQuery userQuery
     * @return Page
     */
    Page<UserDTO> pageQuery(UserQuery userQuery);

    /**
     * Get user detail.
     *
     * @param email email
     * @return user detail
     */
    UserDetailDTO getUserDetail(String email);

    /**
     * Get user.
     *
     * @param email email
     * @return user
     */
    UserDTO get(String email);

    /**
     * Delete a user by email.
     *
     * @param email email
     */
    void deleteUserByEmail(String email);

    /**
     * ????????????.
     *
     * @param user ??????
     * @return ??????????????? User
     */
    UserDO save(UserDO user);

    /**
     * ??????????????????.
     *
     * @param userList ???????????????????????????
     * @return ?????????????????????????????????
     */
    List<UserDO> saveAll(List<UserDO> userList);

    /**
     * ????????????????????????.
     *
     * @param email ????????????
     * @return User
     */
    Optional<UserDO> findUserByEmail(String email);

    /**
     * ??????ID????????????.
     *
     * @param id ??????ID
     * @return User
     */
    Optional<UserDO> findUserById(Long id);

    /**
     * ??????????????????,?????????.
     *
     * @param user ??????
     * @return ????????????
     */
    List<UserDO> queryAll(UserDO user);

    /**
     * ???????????????????????????.
     *
     * @param user     ??????
     * @param pageable ????????????
     * @return Page
     */
    Page<UserDO> query(UserDO user, Pageable pageable);

    /**
     * ??????????????????.
     *
     * @param user User
     * @return ??????????????????
     */
    static Specification specificationOf(final UserDO user) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.isNull(user)) {
                return cb.and(predicates.toArray(new Predicate[predicates.size()]));
            }

            if (!Strings.isNullOrEmpty(user.getEmail())) {
                predicates.add(cb.like(root.get("email"), QueryUtil.like("%", user.getEmail(), "%")));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
