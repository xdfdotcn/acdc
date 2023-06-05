package cn.xdf.acdc.devops.service.process.user;

import cn.xdf.acdc.devops.core.domain.dto.UserAuthorityDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserAuthorityQuery;

import java.util.List;
import java.util.Set;

public interface UserAuthorityService {
    
    /**
     * Query user authority.
     *
     * @param query query
     * @return list
     */
    List<UserAuthorityDTO> queryAll(UserAuthorityQuery query);
    
    /**
     * Add role.
     *
     * @param roleSet role set
     * @param userId userId
     */
    void addRole(Long userId, Set<AuthorityRoleType> roleSet);
    
    /**
     * Delete role by user id.
     *
     * @param userId userId
     */
    void deleteRoleByUserId(Long userId);
    
    /**
     * Rest role .
     *
     * @param userId userId
     * @param roleSet roleSet
     */
    void resetRole(Long userId, Set<AuthorityRoleType> roleSet);
}
