package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.api.util.ApiSecurityUtils;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery.RANGE;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import cn.xdf.acdc.devops.service.util.UserUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1")
@Transactional
public class ProjectController {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private UserService userService;

    /**
     * 查询项目列表.
     *
     * @param projectQuery projectQuery
     * @return Page
     */
    @GetMapping("/projects")
    public PageDTO<ProjectDTO> pagedQuery(final ProjectQuery projectQuery) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        boolean isAdmin = UserUtil.isAdmin(currentUser);
        if (projectQuery.getQueryRange() == RANGE.CURRENT_USER && !isAdmin) {
            projectQuery.setMemberDomainAccount(currentUser.getDomainAccount());
        }
        Page<ProjectDTO> page = projectService.pagedQuery(projectQuery);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }

    /**
     * 根据id查询项目信息.
     *
     * @param id 项目主键id
     * @return cn.xdf.acdc.devops.core.domain.dto.ProjectDTO
     * @date 2022/8/1 4:26 下午
     */
    @GetMapping("/projects/{id}")
    public ProjectDTO getById(@PathVariable("id") final Long id) {
        return projectService.getById(id);
    }

    /**
     * 创建项目.
     *
     * @param projectDTO 项目信息
     * @date 2022/8/2 2:53 下午
     */
    @PostMapping("/projects")
    public void create(@RequestBody final ProjectDTO projectDTO) {
        projectService.create(projectDTO);
    }

    /**
     * 修改项目.
     *
     * @param projectDTO 项目信息
     * @date 2022/8/2 2:53 下午
     */
    @PatchMapping("/projects")
    public void update(@RequestBody final ProjectDTO projectDTO) {
        projectService.update(projectDTO);
    }
}
