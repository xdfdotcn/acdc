package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ProjectServiceIT {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private UserService userService;

    @Autowired
    private RdbService rdbService;

    @Test
    public void testSaveShouldInsertWhenNotExist() {
        ProjectDO project = new ProjectDO();
        project.setId(10L);
        project.setName("project-test");
        project.setDescription("project-desc");
        project.setUpdateTime(new Date().toInstant());
        project.setCreationTime(new Date().toInstant());
        ProjectDO saveResult = projectService.save(project);

        project.setId(saveResult.getId());
        Assertions.assertThat(saveResult).isEqualTo(project);
    }

    @Test
    public void testSaveShouldUpdateWhenExist() {
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setName("test1");
        ProjectDO saveResult1 = projectService.save(project);
        saveResult1.setName("test2");
        ProjectDO saveResult2 = projectService.save(saveResult1);
        Assertions.assertThat(saveResult2.getName()).isEqualTo("test2");
        Assertions.assertThat(projectService.queryAll(new ProjectQuery()).size()).isEqualTo(1);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSaveShouldFailWhenMissingNotNullField() {
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setDescription("desc");
        projectService.save(project);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveShouldFailWhenGivenNull() {
        projectService.save(null);
    }

    @Test
    public void testSaveShouldAutoSetCreationTimeAndUpdateTimeWhenIsNull() {
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setName("project-test");
        project.setDescription("project-desc");
        ProjectDO saveResult = projectService.save(project);

        Assertions.assertThat(saveResult.getCreationTime()).isNotNull();
        Assertions.assertThat(saveResult.getUpdateTime()).isNotNull();
    }

    @Test
    public void testSaveShouldNotAutoRenewCreationTimeWhenUpdate() {
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setName("project-test");
        project.setDescription("project-desc");
        ProjectDO saveResult1 = projectService.save(project);

        saveResult1.setName("project-test-updated");
        ProjectDO saveResult2 = projectRepository.saveAndFlush(saveResult1);

        Assertions.assertThat(saveResult2.getCreationTime()).isEqualTo(saveResult1.getCreationTime());
    }

    @Test
    public void testSaveShouldAutoRenewUpdateTimeWhenUpdate() throws InterruptedException {
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setName("project-test");
        project.setDescription("project-desc");
        ProjectDO saveResult1 = projectService.save(project);

        Thread.sleep(1000);
        saveResult1.setName("project-test-updated");
        // 由于单元测试类的 @Transactional 注解，因此本单元测试方法返回前不会触发 update 行为
        // 故使用此方法强制提交事物，从而强制 updateTimestamp 字段更新
        ProjectDO saveResult2 = projectRepository.saveAndFlush(saveResult1);

        Assertions.assertThat(saveResult2.getUpdateTime()).isAfter(saveResult2.getCreationTime());
    }

    @Test
    public void testSaveAllShouldInsertWhenNotExist() {
        List<ProjectDO> projectList = createProjectList();
        List<ProjectDO> saveResult = projectService.saveAll(projectList);

        Assertions.assertThat(saveResult.size()).isEqualTo(projectList.size());

        for (int i = 0; i < projectList.size(); i++) {
            projectList.get(i).setId(saveResult.get(i).getId());
            Assertions.assertThat(saveResult.get(i)).isEqualTo(projectList.get(i));
        }
    }

    @Test
    public void testSaveAllShouldUpdateWhenExist() {
        List<ProjectDO> projectList = createProjectList();
        List<ProjectDO> saveResult = projectService.saveAll(projectList);
        saveResult.forEach(project -> project.setDescription("test_update"));
        projectService.saveAll(saveResult).forEach(project -> {
            Assertions.assertThat(project.getDescription()).isEqualTo("test_update");
        });
        Assertions.assertThat(projectService.saveAll(saveResult).size()).isEqualTo(projectList.size());
    }

    @Test(expected = DataIntegrityViolationException.class)
    public void testSaveAllShouldFailWhenMissingNotNullField() {
        List<ProjectDO> projectList = createProjectList();
        projectList.forEach(project -> project.setName(null));
        projectService.saveAll(projectList);
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void testSaveAllShouldFailWhenGivenNull() {
        projectService.saveAll(null);
    }

    @Test
    public void testSaveAllShouldDoNothingWhenGivenEmptyList() {
        projectService.saveAll(Collections.emptyList());
        Assertions.assertThat(projectService.queryAll(new ProjectQuery()).isEmpty()).isEqualTo(true);
    }

    @Test
    public void testQueryAllShouldSuccess() {
        List<ProjectDO> projectList = createProjectListByCount(20);
        projectService.saveAll(projectList);
        ProjectQuery projectQuery = new ProjectQuery();
        projectQuery.setName("p1");
        List<ProjectDO> queryResult = projectService.queryAll(projectQuery);
        Assertions.assertThat(queryResult.size()).isEqualTo(10);
        queryResult.forEach(prj -> Assertions.assertThat(prj.getName()).contains("p1"));

        queryResult = projectService.queryAll(new ProjectQuery());
        long p1Count = queryResult.stream().filter(item -> item.getName().contains("p1")).count();
        long p2Count = queryResult.stream().filter(item -> item.getName().contains("p2")).count();
        Assertions.assertThat(queryResult.size()).isEqualTo(20);
        Assertions.assertThat(p1Count).isEqualTo(10);
        Assertions.assertThat(p2Count).isEqualTo(10);
    }

    @Test(expected = NullPointerException.class)
    public void testQueryAllShouldFailWhenGiveNull() {
        projectService.queryAll(null);
    }

    @Test
    public void testQuery() {
        List<ProjectDO> projectList = createProjectList();
        projectService.saveAll(projectList);

        // 分页正常滚动
        ProjectQuery projectQuery = new ProjectQuery();
        projectQuery.setName("t");
        Page<ProjectDO> page = projectService.query(projectQuery, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = projectService.query(projectQuery, createPageRequest(2, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = projectService.query(projectQuery, createPageRequest(3, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(3);

        page = projectService.query(projectQuery, createPageRequest(4, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(1);

        // 过滤条件不存在
        projectQuery.setName("kk-not-exist");
        page = projectService.query(projectQuery, createPageRequest(1, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(0);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(0);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);

        // 更改pageSize,取消传入查询条件
        page = projectService.query(new ProjectQuery(), createPageRequest(1, 10));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(10);

        // 页越界
        projectQuery.setName("t");
        page = projectService.query(projectQuery, createPageRequest(999, 3));
        Assertions.assertThat(page.getTotalPages()).isEqualTo(4);
        Assertions.assertThat(page.getTotalElements()).isEqualTo(10);
        Assertions.assertThat(page.getContent().size()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryShouldFailWhenGivenIllegalPageIndex() {
        projectService.query(new ProjectQuery(), createPageRequest(-999, -666));
    }

    @Test
    public void testQueryShouldThrowExceptionWhenGivenNull() {
        Throwable throwable = Assertions.catchThrowable(() -> projectService.query(null, createPageRequest(1, 1)));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);
        throwable = Assertions.catchThrowable(() -> projectService.query(new ProjectQuery(), null));
        Assertions.assertThat(throwable).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testCascade() {
        // users
        List<UserDO> userList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            UserDO u = new UserDO();
            u.setEmail(i + "test@xdf.cn");
            u.setPassword(UserLoginServiceIT.PASSWD_HASH);
            u.setCreatedBy("admin-test");
            userList.add(u);
        }
        List<UserDO> saveUserResult = userService.saveAll(userList);
        List<RdbDO> rdbList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            RdbDO rdb = new RdbDO();
            rdb.setRdbType("mysql");
            rdb.setName("a cdc-cluster " + i);
            rdb.setUsername("fake-user");
            rdb.setPassword("fake-password");
            rdb.setDescription("desc");
            rdbList.add(rdb);
        }
        List<RdbDO> saveRdbResult = rdbService.saveAll(rdbList);

        // project
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setName("project-test");
        project.setDescription("project-desc");
        project.setUpdateTime(new Date().toInstant());
        project.setCreationTime(new Date().toInstant());
        project.setOwner(saveUserResult.get(1));
        project.setUsers(saveUserResult.stream().collect(Collectors.toSet()));
        project.setRdbs(saveRdbResult.stream().collect(Collectors.toSet()));
        ProjectDO saveProjectResult = projectService.save(project);

        ProjectDO findProject = projectService.findById(saveProjectResult.getId()).get();
        Assertions.assertThat(findProject.getOwner()).isEqualTo(userList.get(1));
        Assertions.assertThat(findProject.getUsers().size()).isEqualTo(3);
        Assertions.assertThat(findProject.getRdbs().size()).isEqualTo(4);

        // 如果更改关系会全量覆盖掉之前的所有关系
        // TODO 明天继续
        project.setRdbs(saveRdbResult.stream().filter(it -> it.getId() == 1L).collect(Collectors.toSet()));
        ProjectDO overRelationResult = projectService.save(project);
        ProjectDO findOverRelationProject = projectService.findById(overRelationResult.getId()).get();

        Assertions.assertThat(findOverRelationProject.getRdbs().size()).isEqualTo(1);
    }

    @Test
    public void testBatchSaveOrUpdateByDiffing() {
        // 初始化用户
        List<UserDO> userList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            UserDO u = new UserDO();
            u.setEmail(i + "test@xdf.cn");
            u.setPassword(UserLoginServiceIT.PASSWD_HASH);
            u.setCreatedBy("admin-test");
            userList.add(u);
        }
        List<UserDO> saveUserResult = userService.saveAll(userList);

        // 初始化项目
        ProjectDO project = new ProjectDO();
        project.setId(1L);
        project.setName("project-test");
        project.setDescription("project-desc");
        project.setUpdateTime(new Date().toInstant());
        project.setCreationTime(new Date().toInstant());
        project.setUsers(saveUserResult.stream().collect(Collectors.toSet()));
        ProjectDO projectSaveResult = projectService.save(project);

        // 删除一个用户
        saveUserResult.remove(0);
        // 创建一个新的用户作为diff
        UserDO newUser = new UserDO();
        newUser.setEmail("newuser@xdf.cn");
        newUser.setPassword(UserLoginServiceIT.PASSWD_HASH);
        newUser.setCreatedBy("system");
        userService.save(newUser);

        // project 变更用户关系，新增加用户，并且删除一个用户
        saveUserResult.add(newUser);
        projectSaveResult.setUsers(saveUserResult.stream().collect(Collectors.toSet()));

        ProjectDO diffingProjectSaveResult = projectService.save(projectSaveResult);

        Assertions.assertThat(diffingProjectSaveResult.getUsers().size()).isEqualTo(3);
        List<UserDO> diffBeforeSaveUserList = saveUserResult.stream().sorted(Comparator.comparing(UserDO::getEmail)).collect(Collectors.toList());
        List<UserDO> diffAfterSaveUserList = diffingProjectSaveResult.getUsers().stream().sorted(Comparator.comparing(UserDO::getEmail))
                .collect(Collectors.toList());
        for (int i = 0; i < diffBeforeSaveUserList.size(); i++) {
            Assertions.assertThat(diffBeforeSaveUserList.get(i).getEmail()).isEqualTo(diffAfterSaveUserList.get(i).getEmail());
        }
    }

    private Pageable createPageRequest(final int pageIndex, final int pageSize) {
        Pageable pageable = PageRequest.of(pageIndex - 1, pageSize, Sort.by(Sort.Order.desc("name")));
        return pageable;
    }

    private List<ProjectDO> createProjectListByCount(int count) {
        Assert.assertTrue(count >= 1);

        List<ProjectDO> projectList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String projectName = i % 2 == 0 ? "p1-" + i : "p2-" + i;
            ProjectDO project = new ProjectDO();
            project.setId(Long.valueOf(i + 1));
            project.setName(projectName);
            project.setDescription("desc");
            projectList.add(project);
        }
        return projectList;
    }

    private List<ProjectDO> createProjectList() {
        List<ProjectDO> projectList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            ProjectDO project = new ProjectDO();
            project.setId(Long.valueOf(i + 1));
            project.setName("test" + i);
            project.setDescription("test");
            projectList.add(project);
        }
        return projectList;
    }

}
