package cn.xdf.acdc.devops.service.process.user;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;

import java.util.List;

public interface UserProcessService {

    /**
     * Get user.
     *
     * @param id id
     * @return UserDTO
     */
    UserDTO getUser(Long id);

    /**
     * 根据域账号获取用户.
     *
     * @param domainAccount domainAccount
     * @return UserDTO
     */
    UserDTO getUserByDomainAccount(String domainAccount);

    /**
     * Whether you are an administrator.
     *
     * @param domainAccount domainAccount
     * @return boolean
     */
    boolean isAdmin(String domainAccount);

    /**
     * 查询用户列表，可根据账号搜索.
     *
     * @param domainAccount 用户域账号
     * @return java.util.List
     * @date 2022/8/1 6:07 下午
     */
    List<UserDTO> listUser(String domainAccount);

    /**
     * 查询用户列表.
     *
     * @param query query
     * @return 用户列表
     */
    List<UserDTO> query(UserQuery query);

    /**
     * 是否为DBA用户.
     *
     * @param email email
     * @return boolean
     */
    boolean isDba(String email);
}
