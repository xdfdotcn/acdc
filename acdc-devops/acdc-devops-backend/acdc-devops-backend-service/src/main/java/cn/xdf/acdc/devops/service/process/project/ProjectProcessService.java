package cn.xdf.acdc.devops.service.process.project;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import org.springframework.data.domain.Page;

import java.util.List;

public interface ProjectProcessService {

    /**
     * 查询项目列表,分页.
     *
     * @param projectQuery  projectQuery
     * @param domainAccount domainAccount
     * @return Page
     */
    Page<ProjectDTO> query(ProjectQuery projectQuery, String domainAccount);

    /**
     * Get project.
     *
     * @param id id
     * @return ProjectDTO
     */
    ProjectDTO getProject(Long id);

    /**
     * 保存项目.
     *
     * @param projectDTO 项目信息
     * @date 2022/8/2 1:58 下午
     */
    void saveProject(ProjectDTO projectDTO);

    /**
     * 修改项目.
     *
     * @param projectDTO 项目信息
     * @date 2022/8/2 1:58 下午
     */
    void updateProject(ProjectDTO projectDTO);

    /**
     * 查询项目下RDB列表.
     *
     * @param projectId 项目id
     * @return java.util.List
     * @date 2022/8/2 5:21 下午
     */
    List<RdbDTO> queryRdbsByProject(Long projectId);

    /**
     * 查询项目下人员列表.
     *
     * @param projectId 项目id
     * @return java.util.List
     * @date 2022/8/2 5:48 下午
     */
    List<UserDTO> queryUsersByProject(Long projectId);

    /**
     * 添加项目人员.
     *
     * @param id       项目id
     * @param userDTOS 用户列表
     * @date 2022/8/2 6:40 下午
     */
    void saveProjectUsers(Long id, List<UserDTO> userDTOS);

    /**
     * 删除项目人员.
     *
     * @param id      项目id
     * @param userIds 用户id集合
     * @date 2022/8/2 6:44 下午
     */
    void deleteProjectUsers(Long id, List<Long> userIds);

    /**
     * 判断是否为项目负责人.
     *
     * @param email email
     * @param id    id
     * @return boolean
     */
    boolean isProjectOwner(Long id, String email);

}
