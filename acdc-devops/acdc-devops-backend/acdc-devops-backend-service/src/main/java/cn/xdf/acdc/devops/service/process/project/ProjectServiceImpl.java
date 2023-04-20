package cn.xdf.acdc.devops.service.process.project;

import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.core.domain.query.ProjectQuery;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.user.UserService;
import cn.xdf.acdc.devops.service.util.UserUtil;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Project;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.jsonwebtoken.lang.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ProjectServiceImpl implements ProjectService {

    @Autowired
    private I18nService i18n;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private UserService userService;

    @Transactional
    @Override
    public ProjectDTO create(final ProjectDTO project) {
        String ownerEmail = project.getOwnerEmail();
        if (StringUtils.isBlank(ownerEmail)) {
            throw new ClientErrorException("param: ownerEmail is required.");
        }

        Long ownerId = null;
        try {
            ownerId = userService.getByEmail(ownerEmail).getId();
        } catch (EntityNotFoundException e) {
            log.warn("can not find a user by email {}, creating", ownerEmail);
            ownerId = userService.create(generateUser(ownerEmail)).getId();
        }

        project.setOwnerId(ownerId);
        ProjectDO projectDO = project.toDO();
        projectDO.addUser(new UserDO(project.getOwnerId()));
        return new ProjectDTO(projectRepository.save(projectDO));
    }

    private UserDetailDTO generateUser(final String ownerEmail) {
        return new UserDetailDTO()
                .setEmail(ownerEmail)
                .setDomainAccount(UserUtil.convertEmailToDomainAccount(ownerEmail))
                .setName(UserUtil.convertEmailToDomainAccount(ownerEmail))
                .setAuthoritySet(Sets.newHashSet(AuthorityRoleType.ROLE_USER));
    }

    @Override
    public void update(final ProjectDTO projectDTO) {
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
            UserDO owner = userService.getByEmail(ownerEmail).toDO();
            projectDO.setOwner(owner);
            //判断新的owner是否已加到project_user关系表中
            Set<UserDO> users = projectDO.getUsers();
            if (!users.contains(owner)) {
                users.add(owner);
                projectDO.setUsers(users);
            }
        }
        projectRepository.save(projectDO);
    }

    @Transactional
    @Override
    public List<ProjectDTO> batchCreate(final List<ProjectDTO> projects) {
        List<ProjectDO> projectDOs = projects.stream()
                .map(ProjectDTO::toDO)
                .collect(Collectors.toList());

        return projectRepository.saveAll(projectDOs).stream()
                .map(ProjectDTO::new)
                .collect(Collectors.toList());
    }

    @Transactional
    @Override
    public Page<ProjectDTO> pagedQuery(final ProjectQuery projectQuery) {
        return projectRepository.pagedQuery(projectQuery).map(ProjectDTO::new);
    }

    @Transactional
    @Override
    public ProjectDTO getById(final Long id) {
        return projectRepository.findById(id)
                .map(ProjectDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Project.NOT_FOUND, id)));
    }

    @Override
    public List<ProjectDTO> getByIds(final List<Long> ids) {
        return projectRepository.findAllById(ids)
                .stream()
                .map(ProjectDTO::new)
                .collect(Collectors.toList());
    }

    @Transactional
    @Override
    public List<ProjectDTO> query(final ProjectQuery projectQuery) {
        return projectRepository.query(projectQuery).stream()
                .map(ProjectDTO::new)
                .collect(Collectors.toList());
    }

    @Transactional
    @Override
    public void createProjectUsers(final Long id, final List<UserDTO> users) {
        if (Collections.isEmpty(users)) {
            return;
        }
        // 查询项目信息
        ProjectDO projectDO = getById(id).toDO();

        // 重新保存用户关系
        List<UserDO> userDOList = Lists.newArrayList();
        users.forEach(userDTO -> {
            UserDO user = userService.getByEmail(userDTO.getEmail()).toDO();
            userDOList.add(user);
        });
        projectDO.setUsers(Sets.newHashSet(userDOList));
        projectRepository.save(projectDO);
    }

    @Transactional
    @Override
    public void deleteProjectUsers(final Long id, final List<Long> userIds) {
        if (CollectionUtils.isEmpty(userIds)) {
            return;
        }
        //查询项目信息
        ProjectDO projectDO = getById(id).toDO();
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
    public List<ProjectDTO> mergeAllProjectsOnOriginalId(final Set<ProjectDetailDTO> projectsDetails) {
        Map<Long, ProjectDO> existOriginalIdEntityMap = projectRepository.findBySource(MetadataSourceType.FROM_PANDORA)
                .stream().collect(Collectors.toMap(ProjectDO::getOriginalId, projectDO -> projectDO));

        Map<Long, ProjectDO> newOriginalIdEntityMap = projectsDetails.stream().map(projectsDetail -> {
            ProjectDO projectDO = existOriginalIdEntityMap.get(projectsDetail.getOriginalId());
            if (Objects.isNull(projectDO)) {
                projectDO = new ProjectDO();
            }
            filledProjectDOWithDetailDTOProperties(projectDO, projectsDetail);
            return projectDO;
        }).collect(Collectors.toMap(ProjectDO::getOriginalId, projectDO -> projectDO));

        // insert or update for each row
        List<ProjectDO> projectSaveResult = projectRepository.saveAll(newOriginalIdEntityMap.values());

        // logical delete
        List<ProjectDO> logicalDeleteProjects = existOriginalIdEntityMap.values().stream()
                .filter(projectDO -> !newOriginalIdEntityMap.containsKey(projectDO.getOriginalId()))
                .peek(projectDO -> projectDO.setDeleted(Boolean.TRUE))
                .collect(Collectors.toList());
        projectRepository.saveAll(logicalDeleteProjects);

        return projectSaveResult.stream().map(ProjectDTO::new).collect(Collectors.toList());
    }

    private void filledProjectDOWithDetailDTOProperties(final ProjectDO projectDO, final ProjectDetailDTO projectsDetail) {
        projectDO.setName(projectsDetail.getName());
        projectDO.setDescription(projectsDetail.getDescription());
        if (Objects.nonNull(projectsDetail.getOwnerId())) {
            projectDO.setOwner(new UserDO(projectsDetail.getOwnerId()));
        }
        projectDO.setSource(projectsDetail.getSource());
        projectDO.setOriginalId(projectsDetail.getOriginalId());
        if (Objects.nonNull(projectsDetail.getUserIds())) {
            projectDO.setUsers(projectsDetail.getUserIds().stream().map(UserDO::new).collect(Collectors.toSet()));
        }
        projectDO.setDeleted(Boolean.FALSE);
    }
}
