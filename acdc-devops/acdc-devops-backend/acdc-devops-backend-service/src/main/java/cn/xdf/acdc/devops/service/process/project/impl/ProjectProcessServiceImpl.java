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
        // ??????????????????????????????, source: ????????????????????????, sink: ???????????????????????????????????????
        if (projectQuery.getQueryRange() == RANGE.CURRENT_USER && !isAdmin) {
            Set<Long> projectIds = userDO.getProjects().stream().map(ProjectDO::getId).collect(Collectors.toSet());
            if (CollectionUtils.isEmpty(projectIds)) {
                return Page.empty();
            }

            projectQuery.setProjectIds(projectIds);
        }

        // ????????????????????????????????????,???????????????mysql ????????????tidb?????????
        // TODO  ??????????????????
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
        //???devops??????????????????????????????
        projectDO.setSource(ProjectSourceType.USER_INPUT);
        projectDO.setOriginalId(1L);
        projectDO.setCreationTime(Instant.now());
        projectDO.setUpdateTime(Instant.now());
        //???owner???????????????-??????????????????
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
            //????????????owner???????????????project_user????????????
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
            //?????????????????????owner?????????
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
        //??????????????????
        ProjectDO projectDO = projectRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
        //????????????????????????
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
        //???????????????????????????user???
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
        //???????????????????????????????????????list
        userDOList.addAll(registeredUsers);
        //?????????????????????????????????????????????????????????????????????
        projectDO.setUsers(Sets.newHashSet(userDOList));
    }

    @Override
    public void deleteProjectUsers(final Long id, final List<Long> userIds) {
        if (CollectionUtils.isEmpty(userIds)) {
            return;
        }
        //??????????????????
        ProjectDO projectDO = projectRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
        List<UserDO> userDOList = Lists.newArrayList(projectDO.getUsers());
        if (CollectionUtils.isEmpty(userDOList)) {
            return;
        }
        //????????????????????????
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
