package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.MysqlDataSourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.core.util.RdbInstanceUtil;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.config.RdbJdbcConfig;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.entity.RdbService;
import cn.xdf.acdc.devops.service.entity.UserService;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.util.UserUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.jsonwebtoken.lang.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Transactional
@Slf4j
@Order(10)
public class RdbProcessServiceImpl implements RdbProcessService, DataSystemMetadataService<RdbDO> {

    private static final Set<String> DATABASE_FILTER_SET = Sets.newHashSet(
            "information_schema",
            "mysql",
            "performance_schema",
            "sys",
            "sys_operator"
    );

    @Autowired
    private UserService userService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private RdbJdbcConfig rdbJdbcConfig;

    @Autowired
    private RdbService rdbService;

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private MysqlHelperService mysqlHelperService;

    @Autowired
    private RdbDatabaseService rdbDatabaseService;

    @Override
    public void saveMysqlInstance(final MysqlDataSourceDTO mysqlDataSourceDTO) {
        Long rdbId = mysqlDataSourceDTO.getRdbId();
        String host = mysqlDataSourceDTO.getHost().trim();
        Integer port = mysqlDataSourceDTO.getPort();

        rdbService.findById(rdbId)
                .orElseThrow(() -> new NotFoundException(
                        ErrorMsg.E_108,
                        String.format("rdbId: %s", rdbId)));

        RdbInstanceDO rdbInstance = rdbInstanceService.findByRdbIdAndHostAndPort(rdbId, host, port)
                .orElseThrow(() -> new NotFoundException(
                        ErrorMsg.E_107,
                        String.format("rdbId: %s, host: %s, port: %s", rdbId, host, port)));

        rdbInstance.setRole(RoleType.DATA_SOURCE);
        rdbInstanceService.save(rdbInstance);
    }

    @Override
    public MysqlDataSourceDTO getRdbMysqlInstance(final Long rdbId) {
        RdbInstanceDO rdbInstance = rdbInstanceService.findDataSourceInstanceByRdbId(rdbId)
                .orElseThrow(() -> new NotFoundException(
                        ErrorMsg.E_107,
                        String.format("rdbId: %s", rdbId)));

        return new MysqlDataSourceDTO(rdbInstance);
    }

    @Override
    public RdbDTO getRdb(final Long id) {
        return rdbRepository.findById(id)
                .map(RdbDTO::toRdbDTO)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public void saveRdb(final RdbDTO rdbDTO) {
        ProjectDO projectDO = projectRepository.findById(rdbDTO.getProjectId())
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", rdbDTO.getProjectId())));
        RdbDO rdbDO = RdbDTO.toRdbDO(rdbDTO);
        rdbDO.setPassword(EncryptUtil.encrypt(rdbDTO.getPassword()));
        rdbDO.setProjects(Sets.newHashSet(projectDO));
        rdbRepository.save(rdbDO);
        rdbDTO.setId(rdbDO.getId());
    }

    @Override
    public void updateRdb(final RdbDTO rdbDTO) {
        RdbDO rdbDO = rdbRepository.findById(rdbDTO.getId())
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", rdbDTO.getId())));
        if (StringUtils.isNotBlank(rdbDTO.getName())) {
            rdbDO.setName(rdbDTO.getName());
        }
        if (StringUtils.isNotBlank(rdbDTO.getDesc())) {
            rdbDO.setDescription(rdbDTO.getDesc());
        }
        if (StringUtils.isNotBlank(rdbDTO.getUsername())) {
            rdbDO.setUsername(rdbDTO.getUsername());
        }
        if (StringUtils.isNotBlank(rdbDTO.getPassword())) {
            String encrypt = EncryptUtil.encrypt(rdbDTO.getPassword());
            rdbDO.setPassword(encrypt);
        }
        rdbRepository.save(rdbDO);
    }

    @Override
    public void incrementalUpdateRdbsWithRelatedDbInstanceAndProject(final List<RdbDO> sourceRdbList) {
        log.info("Begin to incremental update rdbs, A CDC db user is:{} ", rdbJdbcConfig.getUser());

        // todo ?????????????????????
        // check arguments
        List<RdbDO> rdbList = filterData(sourceRdbList);
        Preconditions.checkNotNull(rdbList);
        Preconditions.checkArgument(!rdbList.isEmpty());
        checkArgument(rdbList);

        // 1. diffing rdb,????????????????????????,?????????????????????
        diffingRdb(rdbList);

        // 2. diffing rdbInstance,????????????????????????,?????????????????????,???????????????????????????,???rdb??????????????????????????????
        diffingRdbInstance(rdbList);

        // 4. diffing user,????????????????????????????????????????????????,????????????
        List<UserDO> savedUserList = diffingUser(rdbList);

        // 6. diffing project,????????????????????????????????????????????????,????????????/rdb??????,???????????????????????????,????????????????????????????????????
        diffingProject(rdbList, savedUserList);
    }

    private List<RdbDO> filterData(final List<RdbDO> sourceRdbList) {
        return sourceRdbList.stream()
                .filter(rdb -> !CollectionUtils.isEmpty(rdb.getProjects())).collect(Collectors.toList());
    }

    private void diffingRdb(final List<RdbDO> toSyncRdbList) {
        Map<String, RdbDO> dbRdbMap = rdbRepository.queryAll(new RdbQuery()).stream()
                .filter(rdbDO -> Objects.equals(ProjectSourceType.FROM_PANDORA, rdbDO.getProjects().stream().findFirst().get().getSource()))
                .collect(Collectors.toMap(RdbDO::getName, rdb -> rdb));

        List<RdbDO> toSaveRdbList = toSyncRdbList.stream().map(rdb -> {
            RdbDO dbRdb = dbRdbMap.get(rdb.getName());

            // ????????????????????????
            if (Objects.isNull(dbRdb)) {
                // ???????????????????????????????????????????????????project?????????
                return RdbDO.builder()
                        .rdbType(rdb.getRdbType())
                        .name(rdb.getName())
                        .username(rdbJdbcConfig.getUser())
                        .password(EncryptUtil.encrypt(rdbJdbcConfig.getPassword()))
                        .description(rdb.getDescription())
                        .build();
            }

            dbRdb.setRdbType(rdb.getRdbType());
            dbRdb.setName(rdb.getName());
            dbRdb.setUsername(rdbJdbcConfig.getUser());
            dbRdb.setPassword(EncryptUtil.encrypt(rdbJdbcConfig.getPassword()));
            dbRdb.setDescription(rdb.getDescription());

            return dbRdb;
        }).collect(Collectors.toList());
        List<RdbDO> savedRdbs = rdbService.saveAll(toSaveRdbList);
        Map<String, Long> savedRdbNameIds = savedRdbs.stream().collect(Collectors.toMap(RdbDO::getName, RdbDO::getId));
        toSyncRdbList.forEach(rdbDO -> rdbDO.setId(savedRdbNameIds.get(rdbDO.getName())));
    }

    private void diffingRdbInstance(final List<RdbDO> toSyncRdbList) {

        Map<String, RdbInstanceDO> dbRdbInstanceMap = rdbInstanceService.queryAll(new RdbInstanceDO()).stream()
                .collect(Collectors
                        .toMap(RdbInstanceUtil::rdbInstanceUniqueKeyOf, rdbInstance -> rdbInstance));

        // ??????????????????????????????instance???????????????
        Set<RdbInstanceDO> toSyncRdbInstanceSet = toSyncRdbList.stream()
                .flatMap(rdb -> rdb.getRdbInstances().stream())
                .collect(Collectors.toSet());

        List<RdbInstanceDO> toSaveRdbInstanceList = toSyncRdbInstanceSet.stream().map(rdbInstance -> {
            String key = RdbInstanceUtil.rdbInstanceUniqueKeyOf(rdbInstance);
            RdbInstanceDO dbRdbInstance = dbRdbInstanceMap.get(key);

            RoleType role = rdbInstance.getRole();
            if (Objects.isNull(dbRdbInstance)) {
                return RdbInstanceDO.builder()
                        .host(rdbInstance.getHost())
                        .port(rdbInstance.getPort())
                        .vip(rdbInstance.getVip())
                        .rdb(rdbInstance.getRdb())
                        .role(role)
                        .creationTime(new Date().toInstant())
                        .updateTime(new Date().toInstant())
                        .build();
            }

            Preconditions.checkArgument(
                    dbRdbInstance.getRdb().getId().equals(dbRdbInstance.getRdb().getId()),
                    "The rdb instance foreign key occur changed,id:" + dbRdbInstance.getId()
            );

//            dbRdbInstance.setHost(rdbInstance.getHost());
//            dbRdbInstance.setPort(rdbInstance.getPort());
            dbRdbInstance.setVip(rdbInstance.getVip());
//            dbRdbInstance.setRole(role.getCode());
            dbRdbInstance.setUpdateTime(new Date().toInstant());
            return dbRdbInstance;
        }).collect(Collectors.toList());

        rdbInstanceService.saveAll(toSaveRdbInstanceList);
    }

    private void diffingProject(final List<RdbDO> toSyncRdbList, final List<UserDO> savedUserList) {
        Map<Long, Set<UserDO>> prjUserSetMap = new HashMap<>();
        Map<Long, Set<RdbDO>> prjRdbSetMap = new HashMap<>();
        Map<Long, ProjectDO> prjMap = new HashMap<>();
        toSyncRdbList.stream().flatMap(rdb -> rdb.getProjects().stream())
                .forEach(it -> {
                    prjMap.put(it.getId(), it);
                    prjUserSetMap.computeIfAbsent(it.getId(), key -> Sets.newHashSet()).addAll(it.getUsers());
                    prjRdbSetMap.computeIfAbsent(it.getId(), key -> Sets.newHashSet()).addAll(it.getRdbs());
                });

        // ????????????????????????User????????????????????????????????????????????????????????????????????????????????????id,???pandora ???????????????userId????????????
        // ???????????????????????????????????????
        Map<String, UserDO> savedUserMap = savedUserList.stream()
                .collect(Collectors.toMap(UserDO::getEmail, user -> user));

        Map<Long, ProjectDO> dbProjectMap = projectService.queryAll(new ProjectQuery()).stream()
                .filter(projectDO -> Objects.equals(ProjectSourceType.FROM_PANDORA, projectDO.getSource()))
                .collect(Collectors.toMap(ProjectDO::getOriginalId, prj -> prj));

        List<ProjectDO> toSaveProjectList = prjMap.values().stream().map(it -> {

            // copy project
            ProjectDO newProject = ProjectDO.builder()
                    .originalId(it.getId())
                    .source(ProjectSourceType.FROM_PANDORA)
                    .name(it.getName())
                    .description(it.getDescription())
                    .rdbs(prjRdbSetMap.get(it.getId()))
                    .users(toDbUserSetByEmail(prjUserSetMap.get(it.getId()), savedUserMap))
                    .owner(toDbUserByEmail(prjMap.get(it.getId()).getOwner(), savedUserMap))
                    .creationTime(new Date().toInstant())
                    .updateTime(new Date().toInstant())
                    .build();

            ProjectDO dbProject = dbProjectMap.get(it.getId());
            if (Objects.isNull(dbProject)) {
                return newProject;
            }

            dbProject.setOwner(newProject.getOwner());
            dbProject.setName(newProject.getName());
            dbProject.setDescription(newProject.getDescription());
            dbProject.setUpdateTime(new Date().toInstant());

            //TODO rdb ???????????????????????????????????????????????????????????????CDC??????????????????????????????????????????????????????????????????????????????????????????
            dbProject.getUsers().addAll(newProject.getUsers());
            dbProject.getRdbs().addAll(newProject.getRdbs());

            return dbProject;
        }).collect(Collectors.toList());
        projectService.saveAll(toSaveProjectList);
    }

    private Set<UserDO> toDbUserSetByEmail(final Set<UserDO> toSyncUsers, final Map<String, UserDO> dbUsers) {
        return toSyncUsers.stream().map(user -> toDbUserByEmail(user, dbUsers)).collect(Collectors.toSet());
    }

    private UserDO toDbUserByEmail(final UserDO user, final Map<String, UserDO> dbUsers) {
        if (Objects.isNull(user)) {
            return null;
        }

        String email = UserUtil.convertDomainAccountToEmail(user.getDomainAccount());
        UserDO dbUser = dbUsers.get(email);
        Preconditions.checkNotNull(dbUser);
        return dbUser;
    }

    private List<UserDO> diffingUser(final List<RdbDO> toSyncRdbList) {
        // TODO ??????diff
        Map<String, UserDO> dbUserMap = userService.queryAll(new UserDO()).stream()
                .collect(Collectors.toMap(UserDO::getEmail, user -> user));

        // TODO review4Yufeng13
        Set<UserDO> toSyncUserList = toSyncRdbList.stream()
                .flatMap(rdb -> rdb.getProjects().stream().flatMap(this::getMemberAndOwnerStream))
                .collect(Collectors.toSet());

        List<UserDO> toSaveUserList = toSyncUserList.stream().map(user -> {
            String email = UserUtil.convertDomainAccountToEmail(user.getDomainAccount());
            UserDO dbUser = dbUserMap.get(email);

            UserDO newUser = new UserDO();
            // ?????????????????????:??????pandora????????????firstName
            newUser.setName(user.getName());
            newUser.setEmail(email);
            newUser.setDomainAccount(user.getDomainAccount());
            newUser.setCreationTime(new Date().toInstant());
            newUser.setUpdateTime(new Date().toInstant());

            if (Objects.isNull(dbUser)) {
                return newUser;
            }

            // 1. TODO ???????????????????????????????????????
            // 2. ????????????????????????, by yangzhongkui
            dbUser.setName(user.getName());
            dbUser.setUpdateTime(new Date().toInstant());
            return dbUser;
        }).collect(Collectors.toList());

        return userService.saveAll(toSaveUserList);
    }

    private Stream<UserDO> getMemberAndOwnerStream(final ProjectDO prj) {
        Set<UserDO> result = new HashSet<>();
        UserDO owner = prj.getOwner();
        if (owner != null) {
            result.add(prj.getOwner());
        }
        Set<UserDO> users = prj.getUsers();
        if (!CollectionUtils.isEmpty(users)) {
            result.addAll(users);
        }
        return result.stream();
    }

    private void checkArgument(final List<RdbDO> rdbList) {
        rdbList.forEach(rdb -> {
            // rdb
            Preconditions.checkNotNull(rdb);
            Preconditions.checkArgument(Objects.nonNull(rdb.getId()) && rdb.getId() > 0);
            Preconditions.checkArgument(StringUtils.isNotBlank(rdb.getRdbType()));
            Preconditions.checkArgument(StringUtils.isNotBlank(rdb.getName()));
            // instance
            checkRdbInstanceArgument(rdb.getRdbInstances());
            // prj
            checkProjectArgument(rdb.getProjects());
        });
    }

    private void checkRdbInstanceArgument(final Set<RdbInstanceDO> instances) {
        if (CollectionUtils.isEmpty(instances)) {
            return;
        }

        instances.forEach(ins -> {
            Preconditions.checkNotNull(ins);
            Preconditions.checkArgument(StringUtils.isNotBlank(ins.getHost()));
            Preconditions.checkArgument(Objects.nonNull(ins.getPort()) && ins.getPort() > 0);
            // todo
            // Preconditions.checkArgument(Objects.nonNull(ins.getRole()));
        });
    }

    private void checkProjectArgument(final Set<ProjectDO> projects) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(projects));

        projects.forEach(prj -> {
            Preconditions.checkArgument(!CollectionUtils.isEmpty(prj.getRdbs()));
            Preconditions.checkArgument(Objects.nonNull(prj.getId()) && prj.getId() > 0);
            Preconditions.checkArgument(StringUtils.isNotBlank(prj.getName()));
            checkUserArgument(prj.getUsers());
        });
    }

    private void checkUserArgument(final Set<UserDO> users) {
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        users.forEach(user -> Preconditions.checkArgument(StringUtils.isNotBlank(user.getDomainAccount())));
    }

    @Override
    public void refreshMetadata() {
        List<RdbDO> rdbs = rdbRepository.findAll();
        this.refreshMetadata(rdbs);
    }

    @Override
    public void refreshMetadata(final List<RdbDO> rdbs) {
        if (Collections.isEmpty(rdbs)) {
            return;
        }

        rdbs.forEach(rdb -> {
            List<String> dataBaseNames = showDataBases(rdb);
            if (!CollectionUtils.isEmpty(dataBaseNames)) {
                diffingDataBase(dataBaseNames, rdb.getId());
            }
        });
    }

    private void diffingDataBase(final List<String> databaseList, final Long rdbId) {
        // 1. ??????????????????????????????????????????????????????????????????????????????????????????rename????????????????????????????????????
        // 2. ?????????????????????????????????????????????????????????source????????????????????????????????????
        RdbDatabaseDO query = RdbDatabaseDO.builder().rdb(RdbDO.builder().id(rdbId).build()).build();
        Map<String, RdbDatabaseDO> dbRdbDatabaseMap = rdbDatabaseService.queryAll(query).stream()
                .collect(Collectors.toMap(RdbDatabaseDO::getName, database -> database));

        List<RdbDatabaseDO> toSaveRdbDatabaseList = databaseList.stream()
                .filter(database -> Objects.isNull(dbRdbDatabaseMap.get(database)))
                .collect(Collectors.toList()).stream()
                .map(database -> {
                    RdbDO rdb = new RdbDO();
                    rdb.setId(rdbId);
                    RdbDatabaseDO newRdbDatabase = new RdbDatabaseDO();
                    newRdbDatabase.setRdb(rdb);
                    newRdbDatabase.setName(database);
                    newRdbDatabase.setCreationTime(new Date().toInstant());
                    newRdbDatabase.setUpdateTime(new Date().toInstant());
                    return newRdbDatabase;
                }).collect(Collectors.toList());
        rdbDatabaseService.saveAll(toSaveRdbDatabaseList);
    }

    private List<String> showDataBases(final RdbDO rdb) {
        try {
            return mysqlHelperService.showDataBases(rdb, this::filterDataBase);
        } catch (ServerErrorException e) {
            log.warn("ShowDatabases not available instance dbType: {}, hosts: {}, message: {}", rdb.getRdbType(),
                    rdb.getDbInstances(), e.getMessage());
            return new ArrayList<>();
        }
    }

    private boolean filterDataBase(final String database) {
        return !DATABASE_FILTER_SET.contains(database.toLowerCase());
    }
}
