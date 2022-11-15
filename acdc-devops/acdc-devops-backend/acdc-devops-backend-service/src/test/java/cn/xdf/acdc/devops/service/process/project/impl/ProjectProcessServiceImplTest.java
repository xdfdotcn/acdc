package cn.xdf.acdc.devops.service.process.project.impl;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.process.project.ProjectProcessService;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Set;

// CHECKSTYLE:OFF

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class ProjectProcessServiceImplTest {

    @Autowired
    private ProjectProcessService projectProcessService;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private UserRepository userRepository;

    @Before
    public void init() {
        mockProject(mockUser(), mockRdb());
    }

    private void mockProject(Set<UserDO> users, Set<RdbDO> rdbDOS) {
        ProjectDO projectDO = new ProjectDO();
        projectDO.setId(1L);
        projectDO.setName("测试项目");
        projectDO.setDescription("Test");
        projectDO.setSource(ProjectSourceType.USER_INPUT);
        projectDO.setOriginalId(1L);
        projectDO.setRdbs(rdbDOS);
        projectDO.setUsers(users);
        projectRepository.save(projectDO);
    }

    @Test
    public void testQueryRdbByProject() {
        List<RdbDTO> pageDTO = projectProcessService.queryRdbsByProject(1L);
        Assertions.assertThat(pageDTO.size()).isEqualTo(2);
    }

    @Test
    public void testSaveProjectUsers() {
        UserDTO u1 = new UserDTO();
        u1.setEmail("green@abc.cn");
        UserDTO u2 = new UserDTO();
        u2.setEmail("tom@abc.cn");
        List<UserDTO> userDTOList = Lists.newArrayList(u1, u2);

        projectProcessService.saveProjectUsers(1L, userDTOList);
        List<UserDTO> users = projectProcessService.queryUsersByProject(1L);
        Assertions.assertThat(users.size()).isEqualTo(2);
    }

    @Test
    public void testDeleteProjectUsers() {
        projectProcessService.deleteProjectUsers(1L, Lists.newArrayList(1L));
        List<UserDTO> users = projectProcessService.queryUsersByProject(1L);
        Assertions.assertThat(users.size()).isEqualTo(1);
    }

    @Test
    public void getProject() {
        ProjectDTO project = projectProcessService.getProject(1L);
        Assertions.assertThat(project).isNotNull();
    }

    @Test
    @Transactional
    public void testSaveProject() {
        ProjectDTO dto = new ProjectDTO();
        dto.setName("测试项目");
        dto.setDescription("Test");
        dto.setSource(ProjectSourceType.USER_INPUT);
        dto.setOriginalId(1L);
        dto.setOwnerEmail("tony@abc.cn");
        projectProcessService.saveProject(dto);
        ProjectDTO project = projectProcessService.getProject(2L);
        Assertions.assertThat(project).isNotNull();
    }

    @Test
    public void testUpdateProject() {
        ProjectDTO dto = new ProjectDTO();
        dto.setId(1L);
        dto.setName("测试项目-1");
        dto.setDescription("Test-1");
        dto.setOwnerEmail("judy@abc.cn");
        projectProcessService.updateProject(dto);
        ProjectDTO project = projectProcessService.getProject(1L);
        Assertions.assertThat(project.getOwnerEmail()).isEqualTo("judy@abc.cn");
    }

    @Test
    public void testQueryUsersByProject() {
        List<UserDTO> dtoList = projectProcessService.queryUsersByProject(1L);
        Assertions.assertThat(dtoList.size()).isEqualTo(2);
    }

    @NotNull
    private Set<RdbDO> mockRdb() {
        RdbDO rdbDO = new RdbDO();
        rdbDO.setId(1L);
        rdbDO.setDescription("rdb1");
        rdbDO.setRdbType("mysql");
        rdbDO.setDescription("1");
        rdbDO.setName("test-1");
        rdbDO.setPassword("123");
        rdbDO.setUsername("abc");

        RdbDO rdbDO2 = new RdbDO();
        rdbDO2.setId(2L);
        rdbDO2.setDescription("rdb2");
        rdbDO2.setRdbType("mysql");
        rdbDO2.setDescription("2");
        rdbDO2.setName("test-2");
        rdbDO2.setPassword("123");
        rdbDO2.setUsername("abc");

        Set<RdbDO> rdbDOS = Sets.newHashSet(rdbDO, rdbDO2);
        rdbRepository.saveAll(rdbDOS);
        return rdbDOS;
    }

    private Set<UserDO> mockUser() {
        UserDO u1 = new UserDO();
        u1.setId(1L);
        u1.setCreatedBy("admin");
        u1.setCreationTime(Instant.now());
        u1.setEmail("tony@abc.cn");

        UserDO u2 = new UserDO();
        u2.setId(2L);
        u2.setCreatedBy("admin");
        u2.setCreationTime(Instant.now());
        u2.setEmail("susan@abc.cn");

        Set<UserDO> users = Sets.newHashSet(u1, u2);
        userRepository.saveAll(users);

        return users;
    }
}
