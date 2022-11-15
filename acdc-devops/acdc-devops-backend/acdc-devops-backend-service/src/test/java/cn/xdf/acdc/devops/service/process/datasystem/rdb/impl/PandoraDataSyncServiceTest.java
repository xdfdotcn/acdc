package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.entity.RdbService;
import cn.xdf.acdc.devops.service.entity.UserService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class PandoraDataSyncServiceTest {

    @Autowired
    private RdbProcessService rdbProcessService;

    @Autowired
    private RdbService rdbService;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private UserService userService;

    @Before
    public void setUp() {
        rdbProcessService.incrementalUpdateRdbsWithRelatedDbInstanceAndProject(initData(0, 3, false));
    }

    /**
     * TODO 1. 排查一下,为什么插入完成之后,级联查询失效,正常开发环境不会出现,只有单元测试环境的 h2存在这个问题. TODO 2. @PostConstruct 注解之前可以解决1的问题,但是现在不知道为什么突然,在同时跑多个单测类的时候setup()中插入的数据会一致存在,影响其他单测
     */
//    @Test
    public void testSyncDataFromPandora() {
        // project-->rdb
        Map<String, ProjectDO> prjMap = projectService.queryAll(new ProjectQuery())
                .stream()
                .collect(Collectors.toMap(ProjectDO::getName, prj -> prj));

        Set<String> prj0RdbNames = prjMap.get("project-0").getRdbs().stream().map(RdbDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(prj0RdbNames.size()).isEqualTo(1);
        Assertions.assertThat(prj0RdbNames.contains("rdb-0")).isEqualTo(true);

        Set<String> prj1RdbNames = prjMap.get("project-1").getRdbs().stream().map(RdbDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(prj1RdbNames.size()).isEqualTo(3);
        Assertions.assertThat(prj1RdbNames.contains("rdb-0")).isEqualTo(true);
        Assertions.assertThat(prj1RdbNames.contains("rdb-1")).isEqualTo(true);
        Assertions.assertThat(prj1RdbNames.contains("rdb-2")).isEqualTo(true);

        Set<String> prj2RdbNames = prjMap.get("project-2").getRdbs().stream().map(RdbDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(prj2RdbNames.size()).isEqualTo(1);
        Assertions.assertThat(prj2RdbNames.contains("rdb-1")).isEqualTo(true);

        // rdb-->project
        Map<String, RdbDO> rdbMap = rdbRepository.queryAll(new RdbQuery())
                .stream()
                .collect(Collectors.toMap(RdbDO::getName, rdb -> rdb));

        Set<String> rdb0PrjNames = rdbMap.get("rdb-0").getProjects().stream().map(ProjectDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(rdb0PrjNames.size()).isEqualTo(2);
        Assertions.assertThat(rdb0PrjNames.contains("project-0")).isEqualTo(true);
        Assertions.assertThat(rdb0PrjNames.contains("project-1")).isEqualTo(true);

        Set<String> rdb1PrjNames = rdbMap.get("rdb-1").getProjects().stream().map(ProjectDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(rdb1PrjNames.size()).isEqualTo(2);
        Assertions.assertThat(rdb1PrjNames.contains("project-1")).isEqualTo(true);
        Assertions.assertThat(rdb1PrjNames.contains("project-2")).isEqualTo(true);

        Set<String> rdb2PrjNames = rdbMap.get("rdb-2").getProjects().stream().map(ProjectDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(rdb2PrjNames.size()).isEqualTo(1);
        Assertions.assertThat(rdb2PrjNames.contains("project-1")).isEqualTo(true);

        // project-->user

        Set<String> prj0UserEmails = prjMap.get("project-0").getUsers().stream().map(UserDO::getEmail)
                .collect(Collectors.toSet());
        Assertions.assertThat(prj0UserEmails.size()).isEqualTo(2);
        Assertions.assertThat(prj0UserEmails.contains("y-0@xdf.cn")).isEqualTo(true);
        Assertions.assertThat(prj0UserEmails.contains("y-1@xdf.cn")).isEqualTo(true);

        Set<String> prj1UserEmails = prjMap.get("project-1").getUsers().stream().map(UserDO::getEmail)
                .collect(Collectors.toSet());
        Assertions.assertThat(prj1UserEmails.size()).isEqualTo(2);
        Assertions.assertThat(prj1UserEmails.contains("y-1@xdf.cn")).isEqualTo(true);
        Assertions.assertThat(prj1UserEmails.contains("y-2@xdf.cn")).isEqualTo(true);

        Set<String> prj2UserEmails = prjMap.get("project-2").getUsers().stream().map(UserDO::getEmail)
                .collect(Collectors.toSet());
        Assertions.assertThat(prj2UserEmails.size()).isEqualTo(1);
        Assertions.assertThat(prj2UserEmails.contains("y-1@xdf.cn")).isEqualTo(true);

        // user-->project
        Map<String, UserDO> userMap = userService.queryAll(new UserDO())
                .stream()
                .collect(Collectors.toMap(UserDO::getEmail, user -> user));

        Set<String> user0PrjNames = userMap.get("y-0@xdf.cn").getProjects().stream().map(ProjectDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(user0PrjNames.size()).isEqualTo(1);
        Assertions.assertThat(user0PrjNames.contains("project-0")).isEqualTo(true);

        Set<String> user1PrjNames = userMap.get("y-1@xdf.cn").getProjects().stream().map(ProjectDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(user1PrjNames.size()).isEqualTo(3);
        Assertions.assertThat(user1PrjNames.contains("project-0")).isEqualTo(true);
        Assertions.assertThat(user1PrjNames.contains("project-1")).isEqualTo(true);
        Assertions.assertThat(user1PrjNames.contains("project-2")).isEqualTo(true);

        Set<String> user2PrjNames = userMap.get("y-2@xdf.cn").getProjects().stream().map(ProjectDO::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(user2PrjNames.size()).isEqualTo(1);
        Assertions.assertThat(user2PrjNames.contains("project-1")).isEqualTo(true);

        // databases
        Assertions.assertThat(rdbMap.get("rdb-0").getRdbDatabases().size()).isEqualTo(2);
        Assertions.assertThat(rdbMap.get("rdb-1").getRdbDatabases().size()).isEqualTo(2);
        Assertions.assertThat(rdbMap.get("rdb-2").getRdbDatabases().size()).isEqualTo(2);

        // instances
        Assertions.assertThat(rdbMap.get("rdb-0").getRdbInstances().size()).isEqualTo(1);
        Assertions.assertThat(rdbMap.get("rdb-1").getRdbInstances().size()).isEqualTo(1);
        Assertions.assertThat(rdbMap.get("rdb-2").getRdbInstances().size()).isEqualTo(1);

        // tables
        Assertions.assertThat(
                rdbMap.get("rdb-0").getRdbDatabases().stream().collect(Collectors.toList()).get(0).getRdbTables().size())
                .isEqualTo(2);
        Assertions.assertThat(
                rdbMap.get("rdb-1").getRdbDatabases().stream().collect(Collectors.toList()).get(0).getRdbTables().size())
                .isEqualTo(2);
        Assertions.assertThat(
                rdbMap.get("rdb-2").getRdbDatabases().stream().collect(Collectors.toList()).get(0).getRdbTables().size())
                .isEqualTo(2);
    }

    @Test
    public void testSyncDataFromPandoraShouldUpdateWhenSameUniqueKey() {
        List<RdbDO> rdbList = initData(0, 3, false);

        AtomicInteger i = new AtomicInteger();
        // rdb
        rdbList.forEach(rdb -> rdb.setName("rdb-" + i.getAndIncrement()));

        // user
        rdbList.stream()
                .flatMap(rdb -> rdb.getProjects().stream().flatMap(prj -> prj.getUsers().stream()))
                .collect(Collectors.toSet())
                .forEach(user -> user.setName("user-test"));

        // prj
        rdbList.stream()
                .flatMap(rdb -> rdb.getProjects().stream())
                .collect(Collectors.toSet())
                .forEach(prj -> prj.setName("prj-test"));

        // instance
        rdbList.stream()
                .flatMap(rdb -> rdb.getRdbInstances().stream())
                .forEach(ins -> ins.setVip("vip"));
        rdbProcessService.incrementalUpdateRdbsWithRelatedDbInstanceAndProject(rdbList);

        // rdb
        List<RdbDO> saveRdbList = rdbRepository.queryAll(new RdbQuery());
        Assertions.assertThat(saveRdbList.size()).isEqualTo(3);

        i.set(0);
        saveRdbList.forEach(rdb -> Assertions.assertThat(rdb.getName()).isEqualTo("rdb-" + i.getAndIncrement()));

        // prj
        List<ProjectDO> savePrjList = projectService.queryAll(new ProjectQuery());
        Assertions.assertThat(savePrjList.size()).isEqualTo(3);
        savePrjList.forEach(prj -> Assertions.assertThat(prj.getName()).isEqualTo("prj-test"));

        // user
        List<UserDO> saveUserList = userService.queryAll(new UserDO());
        Assertions.assertThat(saveUserList.size()).isEqualTo(3);

        // instance
        List<RdbInstanceDO> saveInsList = rdbInstanceService.queryAll(new RdbInstanceDO());
        Assertions.assertThat(saveInsList.size()).isEqualTo(3);
        saveInsList.forEach(ins -> Assertions.assertThat(ins.getVip()).isEqualTo("vip"));
    }

    @Test
    public void testSyncDataFromPandoraShouldInsertWhenNewUniqueKey() {
        List<RdbDO> rdbList = initData(3, 6, false);

        rdbProcessService.incrementalUpdateRdbsWithRelatedDbInstanceAndProject(rdbList);

        // rdb
        List<RdbDO> saveRdbList = rdbRepository.queryAll(new RdbQuery());
        Assertions.assertThat(saveRdbList.size()).isEqualTo(6);

        // prj
        List<ProjectDO> savePrjList = projectService.queryAll(new ProjectQuery());
        Assertions.assertThat(savePrjList.size()).isEqualTo(6);

        // user
        List<UserDO> saveUserList = userService.queryAll(new UserDO());
        Assertions.assertThat(saveUserList.size()).isEqualTo(6);

        // instance
        List<RdbInstanceDO> saveInsList = rdbInstanceService.queryAll(new RdbInstanceDO());
        Assertions.assertThat(saveInsList.size()).isEqualTo(6);
    }

    @Test
    public void testSyncDataFromPandoraShouldIgnoreWhenRelationDel() {
        List<RdbDO> rdbList = initData(0, 3, true);

        rdbProcessService.incrementalUpdateRdbsWithRelatedDbInstanceAndProject(rdbList);

        // rdb
        List<RdbDO> saveRdbList = rdbRepository.queryAll(new RdbQuery());
        Assertions.assertThat(saveRdbList.size()).isEqualTo(3);

        // prj
        List<ProjectDO> savePrjList = projectService.queryAll(new ProjectQuery());
        Assertions.assertThat(savePrjList.size()).isEqualTo(3);

        // user
        List<UserDO> saveUserList = userService.queryAll(new UserDO());
        Assertions.assertThat(saveUserList.size()).isEqualTo(3);

        // instance
        List<RdbInstanceDO> saveInsList = rdbInstanceService.queryAll(new RdbInstanceDO());
        Assertions.assertThat(saveInsList.size()).isEqualTo(3);
    }

    @Test
    public void testPassWordEncryption() {
        List<RdbDO> savedRdbList = rdbRepository.queryAll(new RdbQuery());
        String encryptPassword = EncryptUtil.encrypt("irDIaBmO3RhT");
        savedRdbList.forEach(it -> {
            Assertions.assertThat(it.getPassword()).isEqualTo(encryptPassword);
        });
    }

    private List<RdbDO> initData(final int from, final int to, final boolean prune) {
        List<RdbDO> rdbList = Lists.newArrayList();
        List<UserDO> userList = Lists.newArrayList();
        List<ProjectDO> projectList = Lists.newArrayList();
        List<Set<RdbInstanceDO>> rdbInstanceList = Lists.newArrayList();
        // rdb
        for (int i = from; i < to; i++) {
            RdbDO rdb = new RdbDO();
            rdb.setId(Long.valueOf(i + 1));
            rdb.setName("rdb-" + i);
            rdb.setUsername("root_user" + i);
            rdb.setPassword("root_password");
            rdb.setDescription("rdb desc " + i);
            if (i == 1) {
                rdb.setRdbType("mysql");
            } else {
                rdb.setRdbType("tidb");
            }
            rdbList.add(rdb);
        }

        // instance set for each rdb
        for (int i = 0; i < 3; i++) {
            Set instanceSet = new HashSet();
            for (int j = 0; j < 2; j++) {
                RdbInstanceDO rdbInstance = new RdbInstanceDO();
                rdbInstance.setRole(RoleType.MASTER);
                rdbInstance.setHost("10.211.55.2");
                rdbInstance.setPort(3306);
                instanceSet.add(rdbInstance);
            }
            rdbInstanceList.add(instanceSet);
        }

        // project list
        for (int i = from; i < to; i++) {
            ProjectDO project = new ProjectDO();
            project.setId(Long.valueOf(i + 1));
            project.setName("project-" + i);
            project.setSource(ProjectSourceType.FROM_PANDORA);
//            project.setDescription("project desc " + i);
            projectList.add(project);
        }

        // user list
        for (int i = from; i < to; i++) {
            UserDO user = new UserDO();
            user.setId(Long.valueOf(i + 1));
            user.setDomainAccount("y-" + i);
            user.setCreatedBy("system");
            userList.add(user);
        }

        // rdb instance 一对多的关系
        if (!prune) {
            rdbList.get(0).setRdbInstances(getRdbInstance(rdbList.get(0), 0, rdbInstanceList));
            rdbList.get(1).setRdbInstances(getRdbInstance(rdbList.get(1), 1, rdbInstanceList));
            rdbList.get(2).setRdbInstances(getRdbInstance(rdbList.get(2), 2, rdbInstanceList));
        } else {
            Set<RdbInstanceDO> rdb0InstanceSet = getRdbInstance(rdbList.get(0), 0, rdbInstanceList);
            List<RdbInstanceDO> rdb0InstanceList = rdb0InstanceSet.stream().collect(Collectors.toList());
            rdb0InstanceList.remove(0);
            rdbList.get(0).setRdbInstances(rdb0InstanceList.stream().collect(Collectors.toSet()));

            rdbList.get(1).setRdbInstances(Sets.newHashSet());
            rdbList.get(2).setRdbInstances(getRdbInstance(rdbList.get(2), 2, rdbInstanceList));
        }

        // 保存rdb 到project 的多对多关系
        if (!prune) {
            rdbList.get(0).addProject(projectList.get(0));
            rdbList.get(0).addProject(projectList.get(1));

            rdbList.get(1).addProject(projectList.get(1));
            rdbList.get(1).addProject(projectList.get(2));

            rdbList.get(2).addProject(projectList.get(1));
            /**
             *  rdb    prj
             *  1       1   --第一行
             *  1       2
             *
             *  2       2  --第二行
             *  2       3
             *
             *  3       2  --第三行
             *
             * */
            projectList.get(0).addRdb(rdbList.get(0));

            projectList.get(1).addRdb(rdbList.get(0));
            projectList.get(1).addRdb(rdbList.get(1));
            projectList.get(1).addRdb(rdbList.get(2));

            projectList.get(2).addRdb(rdbList.get(1));
        } else {
            rdbList.get(0).addProject(projectList.get(0));
            rdbList.get(0).setProjects(Sets.newHashSet());
            rdbList.get(2).addProject(projectList.get(1));

            projectList.get(0).addRdb(rdbList.get(0));
            projectList.get(1).addRdb(rdbList.get(2));
            projectList.get(2).setRdbs(Sets.newHashSet());
        }

        // 保存用户和project关系
        if (!prune) {
            projectList.get(0).setUsers(getUserSet(userList, 0, 1));
            projectList.get(1).setUsers(getUserSet(userList, 1, 2));
            projectList.get(2).setUsers(getUserSet(userList, 1));

            userList.get(0).setProjects(getProjectSet(projectList, 0));
            userList.get(1).setProjects(getProjectSet(projectList, 0, 1, 2));
            userList.get(2).setProjects(getProjectSet(projectList, 1));
        } else {
            projectList.get(0).setUsers(getUserSet(userList, 1));
            projectList.get(1).setUsers(Sets.newHashSet());
            projectList.get(2).setUsers(getUserSet(userList, 1));

            userList.get(0).setProjects(Sets.newHashSet());
            userList.get(1).setProjects(getProjectSet(projectList, 0, 2));
            userList.get(2).setProjects(Sets.newHashSet());
        }

        return rdbList;
    }

    private Set<ProjectDO> getProjectSet(final List<ProjectDO> projectList, final int... indexArr) {
        Set<ProjectDO> projectSet = Sets.newHashSet();
        for (int i : indexArr) {
            projectSet.add(projectList.get(i));
        }
        return projectSet;
    }

    private Set<UserDO> getUserSet(final List<UserDO> userList, final int... indexArr) {
        Set<UserDO> userSet = Sets.newHashSet();
        for (int i : indexArr) {
            userSet.add(userList.get(i));
        }
        return userSet;
    }

    private Set<RdbDO> getRdbSet(final List<RdbDO> rdbList, final int... indexArr) {
        Set<RdbDO> rdbSet = Sets.newHashSet();
        for (int i : indexArr) {
            rdbSet.add(rdbList.get(i));
        }
        return rdbSet;
    }

    private Set<RdbInstanceDO> getRdbInstance(final RdbDO rdb, final int index,
            final List<Set<RdbInstanceDO>> rdbInstanceList) {
        Set<RdbInstanceDO> instanceSet = rdbInstanceList.get(index);
        instanceSet.forEach(in -> {
            in.setRdb(rdb);
        });
        return instanceSet;
    }

    @Test
    public void testMergeRdbAndUserByProjectId() {
        Map<Long, Set<UserDO>> prjUserMap = new HashMap<>();
        Map<Long, Set<RdbDO>> prjRdbMap = new HashMap<>();
        Set<ProjectDO> prjSet = Sets.newHashSet();
        List<RdbDO> rdbList = initData(0, 3, false);
        rdbList.stream().flatMap(rdb -> rdb.getProjects().stream())
                .forEach(it -> {
                    prjSet.add(it);
                    prjUserMap.computeIfAbsent(it.getId(), key -> Sets.newHashSet()).addAll(it.getUsers());
                    prjRdbMap.computeIfAbsent(it.getId(), kye -> Sets.newHashSet()).addAll(it.getRdbs());
                });

        Assertions.assertThat(prjRdbMap.get(1L).stream().map(RdbDO::getId).collect(Collectors.toList())).contains(1L);
        Assertions.assertThat(prjRdbMap.get(2L).stream().map(RdbDO::getId).collect(Collectors.toList()))
                .contains(1L, 1L, 3L);
        Assertions.assertThat(prjRdbMap.get(3L).stream().map(RdbDO::getId).collect(Collectors.toList())).contains(2L);
    }
}
