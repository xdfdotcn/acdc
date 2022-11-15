package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import java.util.List;
import java.util.Optional;

/**
 * 用户权限,角色.
 */
public interface AuthorityService {

    /**
     * 保存权限角色.
     *
     * @param authority 权限
     * @return 保存成功的 权限
     */
    AuthorityDO save(AuthorityDO authority);

    /**
     * 根据ID查询权限.
     *
     * @param name 权限名称
     * @return 权限
     */
    Optional<AuthorityDO> findByName(String name);

    /**
     * 查询所有的权限.
     *
     * @return 所有的权限列表
     */
    List<AuthorityDO> findAll();
}
