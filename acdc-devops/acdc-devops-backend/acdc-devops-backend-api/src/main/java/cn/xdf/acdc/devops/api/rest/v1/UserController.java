package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 用户管理.
 *
 * @since 2022/8/1 3:20 下午
 */
@RestController
@RequestMapping("api/v1")
public class UserController {

    @Autowired
    private UserService userService;

    @Autowired
    private ProjectService projectService;

    /**
     * Query user.
     *
     * @param userQuery user query
     * @return users
     */
    @GetMapping("/users")
    public List<UserDTO> queryUser(final UserQuery userQuery) {
        return userService.query(userQuery);
    }

    /**
     * Query users in project.
     *
     * @param projectId project id
     * @return users
     */
    @GetMapping("/projects/{id}/users")
    public PageDTO<UserDTO> queryUsersByProject(@PathVariable("id") final Long projectId) {
        List<UserDTO> projectUserList = userService.queryUsersByProjectId(projectId);
        return PageDTO.of(projectUserList, projectUserList.size());
    }

    /**
     * Over write numbers of a project.
     *
     * @param projectId project id
     * @param users     project numbers
     */
    @PostMapping("/projects/{id}/users")
    public void addProjectUsers(@PathVariable("id") final Long projectId, @RequestBody final List<UserDTO> users) {
        projectService.createProjectUsers(projectId, users);
    }

    /**
     * Delete numbers in project.
     *
     * @param projectId project id
     * @param userIds   user ids to delete
     */
    @DeleteMapping("/projects/{id}/users")
    public void deleteProjectUsers(@PathVariable("id") final Long projectId, @RequestBody final List<Long> userIds) {
        projectService.deleteProjectUsers(projectId, userIds);
    }
}
