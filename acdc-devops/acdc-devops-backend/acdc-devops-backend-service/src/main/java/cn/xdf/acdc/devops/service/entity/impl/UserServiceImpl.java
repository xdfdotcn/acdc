package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.entity.UserService;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.NotAuthorizedException;
import cn.xdf.acdc.devops.service.process.user.impl.UserAuthorityService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Connect;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.User;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private I18nService i18n;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserAuthorityService userAuthorityService;

    @Autowired
    private UserDetailsService userDetailsService;

    @Override
    @Transactional
    public UserDTO createUser(final UserDetailDTO userDetail) {
        if (Strings.isNullOrEmpty(userDetail.getEmail())
                || Strings.isNullOrEmpty(userDetail.getName())
                || Strings.isNullOrEmpty(userDetail.getPassword())
                || CollectionUtils.isEmpty(userDetail.getAuthoritySet())
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, userDetail));
        }

        String email = userDetail.getEmail().trim();

        userRepository.findOneByEmailIgnoreCase(userDetail.getEmail()).ifPresent(user -> {
            throw new EntityExistsException(i18n.msg(Connect.CLUSTER_ALREADY_EXISTED, email));
        });

        UserDO toSaveUserDO = userDetail.toUserDO();
        toSaveUserDO.setPassword(EncryptUtil.encrypt(toSaveUserDO.getPassword()));

        UserDO savedUserDO = userRepository.save(toSaveUserDO);
//        userDetail.setId(savedUserDO.getId());
//        savedUserDO.setAuthorities(userDetail.toAuthorityDOSet());

//        userAuthorityService.addRole(userDetail.getId(), userDetail.getAuthoritySet());

        return new UserDTO(savedUserDO);
    }

    @Override
    @Transactional
    public UserDTO updateUser(final UserDetailDTO userDetail) {
        if (Strings.isNullOrEmpty(userDetail.getEmail())
                || Strings.isNullOrEmpty(userDetail.getName())
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, userDetail));
        }

        String email = userDetail.getEmail().trim();
        String username = userDetail.getName();

        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
        foundUser.setName(username);

        UserDO savedUserDO = userRepository.save(foundUser);
        return new UserDTO(savedUserDO);
    }

    @Override
    @Transactional
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
    @Transactional
    public Page<UserDTO> pageQuery(final UserQuery userQuery) {
        Pageable pageable = PagedQuery.ofPage(userQuery.getCurrent(), userQuery.getPageSize());
        return userRepository.queryAll(userQuery, pageable).map(UserDTO::new);
    }

    @Override
    @Transactional
    public UserDetailDTO getUserDetail(final String email) {
        if (Strings.isNullOrEmpty(email)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, email));
        }
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));

        return new UserDetailDTO(foundUser);
    }

    @Override
    @Transactional
    public UserDTO get(final String email) {
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));
        return new UserDTO(foundUser);
    }

    @Override
    @Transactional
    public void deleteUserByEmail(final String email) {
        if (Strings.isNullOrEmpty(email)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, email));
        }
        UserDO foundUser = userRepository.findOneByEmailIgnoreCase(email)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)));

        userRepository.deleteById(foundUser.getId());

        userAuthorityService.deleteRoleByUserId(foundUser.getId());
    }

    @Override
    public UserDO save(final UserDO user) {
        return userRepository.save(user);
    }

    @Override
    public List<UserDO> saveAll(final List<UserDO> userList) {
        return userRepository.saveAll(userList);
    }

    @Override
    public Optional<UserDO> findUserByEmail(final String email) {
        return userRepository.findOneByEmailIgnoreCase(email);
    }

    @Override
    public Optional<UserDO> findUserById(final Long id) {
        return userRepository.findById(id);
    }

    @Override
    public List<UserDO> queryAll(final UserDO user) {
        return userRepository.findAll(UserService.specificationOf(user));
    }

    @Override
    public Page<UserDO> query(final UserDO user, final Pageable pageable) {
        return userRepository.findAll(UserService.specificationOf(user), pageable);
    }
}
