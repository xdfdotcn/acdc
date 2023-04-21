package cn.xdf.acdc.devops.service.process.user;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import org.springframework.data.domain.Page;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface UserService {

    /**
     * Create a new user.
     *
     * @param userDetail userDetail
     * @return UserDTO
     */
    UserDetailDTO create(UserDetailDTO userDetail);

    /**
     * Update user.
     *
     * @param username username
     * @param email    email
     * @return UserDTO
     */
    UserDTO updateUserNameByEmail(String username, String email);

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
     * Query user.
     *
     * @param userQuery user query
     * @return users
     */
    List<UserDTO> query(UserQuery userQuery);

    /**
     * Paging query user.
     *
     * @param userQuery userQuery
     * @return Page
     */
    Page<UserDTO> pagedQuery(UserQuery userQuery);

    /**
     * 查询项目下人员列表.
     *
     * @param projectId 项目id
     * @return java.util.List
     * @date 2022/8/2 5:48 下午
     */
    List<UserDTO> queryUsersByProjectId(Long projectId);

    /**
     * Get user detail.
     *
     * @param email email
     * @return user detail
     */
    UserDetailDTO getDetailByEmail(String email);

    /**
     * Get user by domain account.
     *
     * @param domainAccount domainAccount
     * @return user domainAccount
     */
    UserDTO getByDomainAccount(String domainAccount);

    /**
     * Get user.
     *
     * @param email email
     * @return user
     */
    UserDTO getByEmail(String email);

    /**
     * Get user.
     *
     * @param id id
     * @return user
     */
    UserDTO getById(Long id);

    /**
     * Delete a user by email.
     *
     * @param email email
     */
    void deleteByEmail(String email);

    /**
     * Determine whether you are an administrator.
     *
     * @param domainAccount domain account
     * @return boolean
     */
    boolean isAdmin(String domainAccount);

    /**
     * Determine whether you are an DBA.
     *
     * @param domainAccount domain account
     * @return boolean
     */
    boolean isDBA(String domainAccount);

    /**
     * If domain account exist update the entity, else insert a new one.
     *
     * @param users users
     * @return userDTOS users with id
     */
    List<UserDTO> upsertOnDomainAccount(Collection<UserDTO> users);
}
