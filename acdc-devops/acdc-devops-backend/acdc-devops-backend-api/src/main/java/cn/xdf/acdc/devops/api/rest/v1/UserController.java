package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.security.jwt.JwtTokenProvider;
import cn.xdf.acdc.devops.service.process.project.ProjectProcessService;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
    private UserProcessService userProcessService;

    @Autowired
    private ProjectProcessService projectProcessService;

    @Autowired
    private UserDetailsService userDetailsService;

    @Autowired
    private JwtTokenProvider jwtTokenProvider;


    /**
     * 根据用户账号查询用户信息.
     *
     * @param domainAccount 用户登域账号
     * @return cn.xdf.acdc.devops.core.domain.dto.UserDTO
     * @date 2022/8/1 4:17 下午
     */
    @GetMapping("/users")
    public List<UserDTO> queryUser(@RequestParam(name = "domainAccount", required = false) final String domainAccount) {
        return userProcessService.listUser(domainAccount);
    }

    /**
     * 查询项目下用户列表.
     *
     * @param id 项目主键
     * @return java.util.List
     * @date 2022/8/2 5:46 下午
     */
    @GetMapping("/projects/{id}/users")
    public PageDTO<UserDTO> queryUsersByProject(@PathVariable("id") final Long id) {
        List<UserDTO> projectUserList = projectProcessService.queryUsersByProject(id);
        return PageDTO.of(projectUserList, projectUserList.size());
    }

    /**
     * 添加项目人员.
     *
     * @param id       项目id
     * @param userDTOS 用户列表
     * @date 2022/8/2 2:53 下午
     */
    @PostMapping("/projects/{id}/users")
    public void addProjectUsers(@PathVariable("id") final Long id, @RequestBody final List<UserDTO> userDTOS) {
        projectProcessService.saveProjectUsers(id, userDTOS);
    }

    /**
     * 删除项目人员.
     *
     * @param id      项目id
     * @param userIds 用户id集合
     * @date 2022/8/2 2:53 下午
     */
    @DeleteMapping("/projects/{id}/users")
    public void deleteProjectUsers(@PathVariable("id") final Long id, @RequestBody final List<Long> userIds) {
        projectProcessService.deleteProjectUsers(id, userIds);
    }
}
