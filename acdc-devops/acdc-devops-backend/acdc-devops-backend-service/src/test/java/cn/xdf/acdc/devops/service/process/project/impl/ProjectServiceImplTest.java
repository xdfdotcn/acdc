package cn.xdf.acdc.devops.service.process.project.impl;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.core.domain.enumeration.QueryScope;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@Transactional
public class ProjectServiceImplTest {
    private static final String F_PROJECT = "ACDC_INNER";
    
    @Autowired
    private ProjectService projectService;
    
    @Autowired
    private ProjectRepository projectRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @MockBean
    private UserService userService;
    
    private UserDO user;
    
    @Before
    public void setUp() {
        this.user = saveUser("user");
    }
    
    private UserDO saveUser(final String domainAccount) {
        UserDO user = new UserDO();
        user.setEmail(domainAccount + "@acdc.io");
        user.setName(domainAccount);
        user.setDomainAccount(domainAccount);
        user.setPassword("user");
        user.setCreatedBy("user");
        return userRepository.save(user);
    }
    
    @Test
    public void testCreateShouldAsExcepted() {
        when(userService.getByEmail(user.getEmail())).thenReturn(new UserDTO(user));
        
        ProjectDTO project = projectService.create(generateProject(user));
        
        // assert
        ProjectDTO resultProject = projectService.getById(project.getId());
        Assertions.assertThat(resultProject.getName()).isEqualTo(project.getName());
    }
    
    private ProjectDTO generateProject(final UserDO user) {
        return new ProjectDTO()
                .setName("project")
                .setOwnerId(user.getId())
                .setOwnerEmail(user.getEmail())
                .setDescription("description")
                .setSource(MetadataSourceType.USER_INPUT)
                .setOriginalId(1L);
    }
    
    @Test
    public void testCreateShouldCreateUserWhenUserNotExists() {
        when(userService.getByEmail(anyString())).thenThrow(EntityNotFoundException.class);
        when(userService.create(any())).thenReturn(new UserDetailDTO(user));
        
        UserDO notExistsUser = new UserDO().setEmail("not_exists_user@acdc.io");
        ProjectDTO project = projectService.create(generateProject(notExistsUser));
        
        // assert
        ProjectDTO resultProject = projectService.getById(project.getId());
        Assertions.assertThat(resultProject.getName()).isEqualTo(project.getName());
        
        ArgumentCaptor<UserDetailDTO> sqlCaptor = ArgumentCaptor.forClass(UserDetailDTO.class);
        verify(userService).create(sqlCaptor.capture());
        Assertions.assertThat(sqlCaptor.getValue().getEmail()).isEqualTo(notExistsUser.getEmail());
    }
    
    @Test
    public void testBatchCreate() {
        List<ProjectDTO> toCreateProjects = new ArrayList();
        for (int i = 0; i < 3; i++) {
            toCreateProjects.add(generateProject(user));
        }
        
        projectService.batchCreate(toCreateProjects);
        
        // assert
        Assertions.assertThat(getProjectTotalCount()).isEqualTo(toCreateProjects.size());
    }
    
    @Test
    public void testPagedQuery() {
        List<ProjectDTO> toCreateProjects = new ArrayList();
        for (int i = 0; i < 10; i++) {
            toCreateProjects.add(generateProject(user));
        }
        projectService.batchCreate(toCreateProjects);
        
        ProjectQuery query = new ProjectQuery();
        query.setOwnerDomainAccount(user.getDomainAccount());
        query.setPageSize(3);
        query.setCurrent(1);
        
        Page<ProjectDTO> queriedProjects = projectService.pagedQuery(query);
        
        Assertions.assertThat(queriedProjects.getSize()).isEqualTo(query.getPageSize());
        Assertions.assertThat(queriedProjects.getNumber()).isEqualTo(query.getCurrent() - 1);
        Assertions.assertThat(queriedProjects.getTotalElements()).isEqualTo(toCreateProjects.size());
    }
    
    @Test
    public void testQueryWhenSpecifyProjectUser() {
        // mock
        when(userService.getByEmail(user.getEmail())).thenReturn(new UserDTO(user));
        ProjectDTO createdProject = projectService.create(generateProject(user));
        
        List<UserDTO> createdUsers = new ArrayList<>();
        UserDTO projectUser = new UserDTO(saveUser("other_user"));
        createdUsers.add(projectUser);
        when(userService.getByEmail(projectUser.getEmail())).thenReturn(projectUser);
        projectService.createProjectUsers(createdProject.getId(), createdUsers);
        
        // execute
        ProjectQuery query = new ProjectQuery();
        query.setScope(QueryScope.CURRENT_USER);
        query.setMemberDomainAccount(projectUser.getDomainAccount());
        List<ProjectDTO> queriedProjects = projectService.query(query);
        
        // assert
        Assertions.assertThat(queriedProjects).hasSize(1);
        Assertions.assertThat(queriedProjects.get(0).getId()).isEqualTo(createdProject.getId());
    }
    
    @Test
    public void testGetById() {
        when(userService.getByEmail(user.getEmail())).thenReturn(new UserDTO(user));
        ProjectDTO createdProject = projectService.create(generateProject(user));
        
        ProjectDTO resultProject = projectService.getById(createdProject.getId());
        
        // assert
        Assertions.assertThat(resultProject.getName()).isEqualTo(createdProject.getName());
    }
    
    @Test
    public void testGetByIds() {
        List<ProjectDTO> toCreateProjects = new ArrayList();
        for (int i = 0; i < 10; i++) {
            toCreateProjects.add(generateProject(user));
        }
        List<ProjectDTO> createdProjects = projectService.batchCreate(toCreateProjects);
        
        List<ProjectDTO> resultProjects = projectService.getByIds(createdProjects.stream().map(ProjectDTO::getId).collect(Collectors.toList()));
        
        // assert
        for (int i = 0; i < resultProjects.size(); i++) {
            Assertions.assertThat(resultProjects.get(i).getName()).isEqualTo(createdProjects.get(i).getName());
        }
    }
    
    @Test
    public void testQuery() {
        List<ProjectDTO> toCreateProjects = new ArrayList();
        for (int i = 0; i < 10; i++) {
            toCreateProjects.add(generateProject(user));
        }
        projectService.batchCreate(toCreateProjects);
        
        // assert
        ProjectQuery query = new ProjectQuery();
        query.setOwnerDomainAccount(user.getDomainAccount());
        List<ProjectDTO> queriedProjects = projectService.query(query);
        
        Assertions.assertThat(queriedProjects.size()).isEqualTo(toCreateProjects.size());
    }
    
    @Test
    public void testCreateProjectUsers() {
        when(userService.getByEmail(user.getEmail())).thenReturn(new UserDTO(user));
        ProjectDTO createdProject = projectService.create(generateProject(user));
        
        List<UserDTO> createdUsers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            createdUsers.add(new UserDTO(saveUser("user_" + i)));
            when(userService.getByEmail(createdUsers.get(i).getEmail())).thenReturn(createdUsers.get(i));
        }
        
        projectService.createProjectUsers(createdProject.getId(), createdUsers);
        
        // assert
        ProjectDO resultProject = projectRepository.getOne(createdProject.getId());
        Assertions.assertThat(resultProject.getUsers().size()).isEqualTo(createdUsers.size());
    }
    
    @Test
    public void testDeleteProjectUsers() {
    
    }
    
    @Test
    public void testMergeAllProjectsOnOriginalId() {
        when(userService.getByEmail(user.getEmail())).thenReturn(new UserDTO(user));
        
        List<ProjectDTO> createdProjects = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ProjectDTO toCreateProject = generateProject(user);
            toCreateProject.setSource(MetadataSourceType.FROM_PANDORA);
            toCreateProject.setOriginalId(Long.valueOf(i));
            createdProjects.add(projectService.create(toCreateProject));
        }
        
        // to be delete original id: 0, 1
        // to be update original id: 2, 3, 4
        // to be create original id: 5, 6
        Set<ProjectDetailDTO> toToMergeProjects = new HashSet<>();
        for (int i = 2; i < 7; i++) {
            ProjectDetailDTO toMergeProject = new ProjectDetailDTO()
                    .setSource(MetadataSourceType.FROM_PANDORA)
                    .setOriginalId(Long.valueOf(i))
                    .setName("new project " + i)
                    .setOwnerId(user.getId());
            toMergeProject.getUserIds().add(user.getId());
            toToMergeProjects.add(toMergeProject);
        }
        
        List<ProjectDTO> mergedProjects = projectService.mergeAllProjectsOnOriginalId(toToMergeProjects);
        
        for (int i = 0; i < 2; i++) {
            ProjectDO projectDO = projectRepository.getOne(createdProjects.get(i).getId());
            Assertions.assertThat(projectDO.getDeleted());
        }
        
        mergedProjects.forEach(each -> {
            ProjectDO projectDO = projectRepository.getOne(each.getId());
            Assertions.assertThat(projectDO.getName()).isEqualTo(each.getName());
            Assertions.assertThat(projectDO.getDeleted()).isFalse();
        });
    }
    
    private Long getProjectTotalCount() {
        return projectRepository.findAll()
                .stream().filter(it -> !it.getName().equals(F_PROJECT))
                .count();
    }
}
