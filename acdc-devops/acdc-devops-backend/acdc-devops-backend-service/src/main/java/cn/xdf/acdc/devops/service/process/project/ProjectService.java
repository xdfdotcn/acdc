package cn.xdf.acdc.devops.service.process.project;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import org.springframework.data.domain.Page;

import java.util.List;
import java.util.Set;

public interface ProjectService {
    
    /**
     * 创建项目.
     *
     * @param project 项目model类
     * @return 插入数据库成功的 Project
     */
    ProjectDTO create(ProjectDTO project);
    
    /**
     * Update project.
     *
     * @param projectDTO project DTO
     */
    void update(ProjectDTO projectDTO);
    
    /**
     * 批量创建项目.
     *
     * @param projects 批量创建的项目集合
     * @return 插入数据库成功的项目集合
     */
    List<ProjectDTO> batchCreate(List<ProjectDTO> projects);
    
    /**
     * 根据条件,获取分页的项目列表.
     *
     * @param projectQuery 查询条件model类
     * @return 项目分页列表
     */
    Page<ProjectDTO> pagedQuery(ProjectQuery projectQuery);
    
    /**
     * 根据ID查询项目.
     *
     * @param id 主键
     * @return 项目
     */
    ProjectDTO getById(Long id);
    
    /**
     * Get project list.
     *
     * @param ids primary id list
     * @return project list
     */
    List<ProjectDTO> getByIds(List<Long> ids);
    
    /**
     * 查询所有项目.
     *
     * @param projectQuery projectQuery
     * @return 所有项目列表
     */
    List<ProjectDTO> query(ProjectQuery projectQuery);
    
    /**
     * 添加项目人员.
     *
     * @param id 项目id
     * @param userDTOS 用户列表
     * @date 2022/8/2 6:40 下午
     */
    void createProjectUsers(Long id, List<UserDTO> userDTOS);
    
    /**
     * 删除项目人员.
     *
     * @param id 项目id
     * @param userIds 用户id集合
     * @date 2022/8/2 6:44 下午
     */
    void deleteProjectUsers(Long id, List<Long> userIds);
    
    /**
     * Merge all projects on original id, maybe insert, update, delete project record.
     *
     * @param projectsDetails projects' details
     * @return project DTO list
     */
    List<ProjectDTO> mergeAllProjectsOnOriginalId(Set<ProjectDetailDTO> projectsDetails);
}
