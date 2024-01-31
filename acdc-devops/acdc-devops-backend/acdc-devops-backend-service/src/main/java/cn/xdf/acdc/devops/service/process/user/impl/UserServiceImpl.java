package cn.xdf.acdc.devops.service.process.user.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserAuthorityQuery;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.NotAuthorizedException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.user.UserAuthorityService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.util.UserUtil;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.User;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;

@Service
public class UserServiceImpl implements UserService {
    
    @Autowired
    private I18nService i18n;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private UserAuthorityService userAuthorityService;
    
    @Transactional
    @Override
    public UserDetailDTO create(final UserDetailDTO userDetail) {
        if (Strings.isNullOrEmpty(userDetail.getEmail())
                || Strings.isNullOrEmpty(userDetail.getName())
                || Strings.isNullOrEmpty(userDetail.getPassword())
                || CollectionUtils.isEmpty(userDetail.getAuthoritySet())
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
        }
        
        String email = userDetail.getEmail().trim();
        
        userRepository.findOneByEmailIgnoreCase(userDetail.getEmail()).ifPresent(user -> {
            throw new EntityExistsException(i18n.msg(I18nKey.User.ALREADY_EXISTED, email));
        });
        
        UserDO toSaveUserDO = userDetail.toDO();
        toSaveUserDO.setPassword(EncryptUtil.encrypt(toSaveUserDO.getPassword()));
        
        UserDO savedUserDO = userRepository.save(toSaveUserDO);
        
        return new UserDetailDTO(savedUserDO);
    }
    
    @Transactional
    @Override
    public UserDTO updateUserNameByEmail(final String username, final String email) {
        if (Strings.isNullOrEmpty(email)
                || Strings.isNullOrEmpty(username)
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, String.format("username: %s, email: %s", username, email)));
        }
        
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
        foundUser.setName(username);
        
        UserDO savedUserDO = userRepository.save(foundUser);
        return new UserDTO(savedUserDO);
    }
    
    @Transactional
    @Override
    public void resetPassword(final String email, final String oldPassword, final String newPassword) {
        if (Strings.isNullOrEmpty(email)
                || Strings.isNullOrEmpty(oldPassword)
                || Strings.isNullOrEmpty(newPassword)
        ) {
            String message = String.format("email: %s, oldPassword: %s, newPassword: %s", email, oldPassword, newPassword);
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, message));
        }
        
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email.trim())
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
        
        if (!Objects.equals(oldPassword.trim(), EncryptUtil.decrypt(foundUser.getPassword()))) {
            throw new NotAuthorizedException(i18n.msg(User.ERROR_ORIGINAL_PASSWORD));
        }
        
        foundUser.setPassword(EncryptUtil.encrypt(newPassword));
        userRepository.save(foundUser);
    }
    
    @Transactional
    @Override
    public void resetRole(final String email, final Set<AuthorityRoleType> roleTypeSet) {
        if (CollectionUtils.isEmpty(roleTypeSet)) {
            throw new ClientErrorException(i18n.msg(User.ERROR_ROLE, roleTypeSet));
        }
        roleTypeSet.add(AuthorityRoleType.ROLE_USER);
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
        
        Set<AuthorityDO> toResetAuthoritySet = roleTypeSet.stream()
                .map(it -> new AuthorityDO(it.name()))
                .collect(Collectors.toSet());
        
        foundUser.setAuthorities(toResetAuthoritySet);
        userRepository.save(foundUser);
    }
    
    @Override
    public List<UserDTO> query(final UserQuery userQuery) {
        return userRepository.query(userQuery).stream().map(UserDTO::new).collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public Page<UserDTO> pagedQuery(final UserQuery userQuery) {
        return userRepository.pagedQuery(userQuery).map(UserDTO::new);
    }
    
    @Override
    public List<UserDTO> queryUsersByProjectId(final Long projectId) {
        UserQuery query = new UserQuery();
        query.setProjectId(projectId);
        return userRepository.query(query)
                .stream()
                .map(UserDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public UserDetailDTO getDetailByEmail(final String email) {
        return userRepository.findOneByEmailIgnoreCase(email)
                .map(UserDetailDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
    }
    
    @Transactional
    @Override
    public UserDTO getByDomainAccount(final String domainAccount) {
        return userRepository.findOneByDomainAccountIgnoreCase(domainAccount)
                .map(UserDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, domainAccount)));
    }
    
    @Transactional
    @Override
    public UserDTO getByEmail(final String email) {
        return userRepository.findOneByEmailIgnoreCase(email)
                .map(UserDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
    }
    
    @Transactional
    @Override
    public UserDTO getById(final Long id) {
        return userRepository.findById(id)
                .map(UserDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, id)));
    }
    
    @Transactional
    @Override
    public void deleteByEmail(final String email) {
        if (Strings.isNullOrEmpty(email)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, email));
        }
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
        
        userRepository.deleteById(foundUser.getId());
        
        userAuthorityService.deleteRoleByUserId(foundUser.getId());
    }
    
    @Transactional
    @Override
    public boolean isAdmin(final String domainAccount) {
        UserDO user = userRepository.findOneByDomainAccountIgnoreCase(domainAccount)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Client.INVALID_PARAMETER, domainAccount)));
        return UserUtil.isAdmin(user);
    }
    
    @Transactional
    @Override
    public boolean isDBA(final String domainAccount) {
        UserAuthorityQuery userAuthorityQuery = new UserAuthorityQuery()
                .setAuthorityRoleTypes(Sets.newHashSet(AuthorityRoleType.ROLE_DBA));
        
        Set<Long> userIds = userAuthorityService.queryAll(userAuthorityQuery)
                .stream().map(it -> it.getUserId()).collect(Collectors.toSet());
        
        if (CollectionUtils.isEmpty(userIds)) {
            throw new ServerErrorException("User of the dba role, must not be empty,please check your config");
        }
        
        UserQuery userQuery = new UserQuery()
                .setUserIds(userIds);
        
        return userRepository.query(userQuery).stream()
                .map(UserDO::getDomainAccount)
                .collect(Collectors.toSet())
                .contains(domainAccount);
    }
    
    @Override
    public List<UserDTO> upsertOnDomainAccount(final Collection<UserDTO> users) {
        Map<String, UserDO> existDomainAccountEntityMap = userRepository.findAll().stream()
                .collect(Collectors.toMap(UserDO::getDomainAccount, user -> user));
        
        List<UserDO> result = users.stream().map(userDTO -> {
            UserDO userDO = existDomainAccountEntityMap.get(userDTO.getDomainAccount());
            // if userDO is null, it is a new one without id and relations, which will be inserted, otherwise updated.
            if (Objects.isNull(userDO)) {
                userDO = new UserDO();
            }
            filledUserDOWithDTOProperties(userDTO, userDO);
            return userDO;
        }).collect(Collectors.toList());
        
        List<UserDO> userDOS = userRepository.saveAll(result);
        return userDOS.stream().map(UserDTO::new).collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<UserDTO> getDbaApprovalUsers() {
        UserAuthorityQuery userAuthorityQuery = new UserAuthorityQuery()
                .setAuthorityRoleTypes(Sets.newHashSet(AuthorityRoleType.ROLE_DBA));
        
        Set<Long> userIds = userAuthorityService.queryAll(userAuthorityQuery)
                .stream().map(it -> it.getUserId()).collect(Collectors.toSet());
        
        if (CollectionUtils.isEmpty(userIds)) {
            throw new ClientErrorException("The DBA approve users must not be empty");
        }
        
        UserQuery userQuery = new UserQuery()
                .setUserIds(userIds);
        
        return userRepository.query(userQuery).stream()
                .map(UserDTO::new)
                .collect(Collectors.toList());
    }
    
    private void filledUserDOWithDTOProperties(final UserDTO userDTO, final UserDO newUser) {
        newUser.setName(userDTO.getName());
        newUser.setEmail(userDTO.getEmail());
        newUser.setDomainAccount(userDTO.getDomainAccount());
        newUser.setCreatedBy(SystemConstant.ACDC);
        newUser.setUpdatedBy(SystemConstant.ACDC);
    }
}
