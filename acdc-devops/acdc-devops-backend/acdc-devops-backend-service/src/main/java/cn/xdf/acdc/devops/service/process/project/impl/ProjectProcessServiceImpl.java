package cn.xdf.acdc.devops.service.process.project.impl;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ProjectSourceType;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery.RANGE;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.project.ProjectProcessService;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.jsonwebtoken.lang.Collections;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
public class ProjectProcessServiceImpl implements ProjectProcessService {

    private static final String SYSTEM = "system";

    private static final String EMAIL_SEPARATOR = "@";

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserProcessService userProcessService;

    @Override
    public Page<ProjectDTO> query(final ProjectQuery projectQuery, final String domainAccount) {
        Pageable pageable = PagedQuery.ofPage(projectQuery.getCurrent(), projectQuery.getPageSize());

        UserDO userDO = userProcessService.getUserByDomainAccount(domainAccount).toUserDO();
        boolean isAdmin = userProcessService.isAdmin(domainAccount);
        // 判断选择的项目的来源, source: 可以查询所有项目, sink: 只能查询用户能关联到的项目
        if (projectQuery.getQueryRange() == RANGE.CURRENT_USER && !isAdmin) {
            Set<Long> projectIds = userDO.getProjects().stream().map(ProjectDO::getId).collect(Collectors.toSet());
            if (CollectionUtils.isEmpty(projectIds)) {
                return Page.empty();
            }

            projectQuery.setProjectIds(projectIds);
        }

        // 对所有的查询结果进行过滤,必须为包含mysql 或者包含tidb的项目
        // TODO  性能需要优化
//        Set<Long> rdbProjectId = projectRepository.queryAll(projectQuery).stream()
//                .filter(it -> isContainsMysqlOrTidb(it.getRdbs()))
//                .map(ProjectDO::getId)
//                .collect(Collectors.toSet());
//        projectQuery.setProjectIds(rdbProjectId);

        return projectRepository.queryAll(projectQuery, pageable).map(ProjectDTO::new);
    }

    @Override
    public ProjectDTO getProject(final Long id) {
        return projectRepository.findById(id)
                .map(ProjectDTO::toProjectDTO)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public void saveProject(final ProjectDTO projectDTO) {
        String ownerEmail = projectDTO.getOwnerEmail();
        if (StringUtils.isBlank(ownerEmail)) {
            throw new ClientErrorException("param: ownerEmail is required.");
        }
        UserDO owner = userRepository.findOneByEmailIgnoreCase(ownerEmail).orElseGet(() -> saveOwner(ownerEmail));
        ProjectDO projectDO = ProjectDTO.toProjectDO(projectDTO);
        projectDO.setOwner(owner);
        //从devops平台添加的数据固定值
        projectDO.setSource(ProjectSourceType.USER_INPUT);
        projectDO.setOriginalId(1L);
        projectDO.setCreationTime(Instant.now());
        projectDO.setUpdateTime(Instant.now());
        //将owner添加到用户-项目关系表中
        projectDO.setUsers(Sets.newHashSet(owner));
        projectRepository.save(projectDO);
    }

    @Override
    public void updateProject(final ProjectDTO projectDTO) {
        ProjectDO projectDO = projectRepository.findById(projectDTO.getId())
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", projectDTO.getId())));
        projectDO.setName(projectDTO.getName());
        projectDO.setDescription(projectDTO.getDescription());
        if (StringUtils.isNotBlank(projectDTO.getName())) {
            projectDO.setName(projectDTO.getName());
        }
        if (StringUtils.isNotBlank(projectDTO.getDescription())) {
            projectDO.setDescription(projectDTO.getDescription());
        }
        String ownerEmail = projectDTO.getOwnerEmail();
        UserDO originalOwner = projectDO.getOwner();
        String originalOwnerEmail = Objects.isNull(originalOwner) ? "" : originalOwner.getEmail();
        if (!Objects.equals(originalOwnerEmail, projectDTO.getOwnerEmail())) {
            UserDO owner = userRepository.findOneByEmailIgnoreCase(ownerEmail).orElseGet(() -> saveOwner(ownerEmail));
            projectDO.setOwner(owner);
            //判断新的owner是否已加到project_user关系表中
            Set<UserDO> users = projectDO.getUsers();
            if (!users.contains(owner)) {
                users.add(owner);
                projectDO.setUsers(users);
            }
        }
        projectDO.setUpdateTime(Instant.now());
        projectRepository.save(projectDO);
    }

    @Override
    public List<RdbDTO> queryRdbsByProject(final Long projectId) {
        ProjectDO projectDO = projectRepository.findById(projectId)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", projectId)));
        Set<RdbDO> rdbs = projectDO.getRdbs();
        List<RdbDTO> rdbDTOList = Lists.newArrayList();
        if (Collections.isEmpty(rdbs)) {
            return rdbDTOList;
        }
        rdbs.forEach(rdbDO -> rdbDTOList.add(RdbDTO.toRdbDTO(rdbDO)));
        return rdbDTOList;
    }

    @Override
    public List<UserDTO> queryUsersByProject(final Long projectId) {
        ProjectDO projectDO = projectRepository.findById(projectId)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", projectId)));
        Set<UserDO> users = projectDO.getUsers();
        List<UserDTO> userDTOList = Lists.newArrayList();
        if (Collections.isEmpty(users)) {
            return userDTOList;
        }
        users.forEach(user -> {
            UserDTO dto = new UserDTO(user);
            //设置用户是否为owner的标识
            UserDO owner = projectDO.getOwner();
            if (Objects.isNull(owner)) {
                dto.setOwnerFlag(0);
            } else {
                if (user.getId().equals(owner.getId())) {
                    dto.setOwnerFlag(1);
                } else {
                    dto.setOwnerFlag(0);
                }
            }
            userDTOList.add(dto);
        });
        return userDTOList;
    }

    @Override
    public void saveProjectUsers(final Long id, final List<UserDTO> userDTOS) {
        if (Collections.isEmpty(userDTOS)) {
            return;
        }
        //查询项目信息
        ProjectDO projectDO = projectRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
        //筛选未注册的用户
        List<UserDO> userDOList = Lists.newArrayList();
        List<UserDO> unRegisterUsers = Lists.newArrayList();
        userDTOS.forEach(userDTO -> {
            Optional<UserDO> user = userRepository.findOneByEmailIgnoreCase(userDTO.getEmail());
            if (user.isPresent()) {
                userDOList.add(user.get());
            } else {
                unRegisterUsers.add(userDTO.toUserDO());
            }
        });
        List<UserDO> registeredUsers = Lists.newArrayList();
        //封装用户信息，插入user表
        if (!CollectionUtils.isEmpty(unRegisterUsers)) {
            userDOList.removeAll(unRegisterUsers);
            unRegisterUsers.forEach(unRegisterUser -> {
                String email = unRegisterUser.getEmail();
                if (StringUtils.isBlank(email)) {
                    throw new ClientErrorException("miss parameter of email");
                }
                unRegisterUser.setCreatedBy(SYSTEM);
                unRegisterUser.setCreationTime(Instant.now());
            });
            registeredUsers = userRepository.saveAll(unRegisterUsers);
        }
        //用户注册完成后，重新加回到list
        userDOList.addAll(registeredUsers);
        //前端传入的是修改好的用户集合，直接全量替换即可
        projectDO.setUsers(Sets.newHashSet(userDOList));
    }

    @Override
    public void deleteProjectUsers(final Long id, final List<Long> userIds) {
        if (CollectionUtils.isEmpty(userIds)) {
            return;
        }
        //查询项目信息
        ProjectDO projectDO = projectRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
        List<UserDO> userDOList = Lists.newArrayList(projectDO.getUsers());
        if (CollectionUtils.isEmpty(userDOList)) {
            return;
        }
        //移除待删除的用户
        List<UserDO> remainUsers = userDOList.stream()
                .filter(userDO -> !userIds.contains(userDO.getId())).collect(Collectors.toList());
        projectDO.setUsers(Sets.newHashSet(remainUsers));
        projectRepository.save(projectDO);
    }

    @Override
    public boolean isProjectOwner(final Long id, final String email) {
        return getProject(id).getOwnerEmail().equals(email);
    }

    @NotNull
    private UserDO saveOwner(final String ownerEmail) {
        UserDO owner;
        owner = new UserDO();
        owner.setEmail(ownerEmail);
        owner.setCreationTime(Instant.now());
        owner.setUpdateTime(Instant.now());
        userRepository.save(owner);
        return owner;
    }

    private boolean isContainsMysqlOrTidb(final Set<RdbDO> rdbs) {
        if (CollectionUtils.isEmpty(rdbs)) {
            return false;
        }
        for (RdbDO rdb : rdbs) {
            if (DataSystemType.MYSQL.getName().equals(rdb.getRdbType())
                    || DataSystemType.TIDB.getName().equals(rdb.getRdbType())
            ) {
                return true;
            }
        }
        return false;
    }
}
