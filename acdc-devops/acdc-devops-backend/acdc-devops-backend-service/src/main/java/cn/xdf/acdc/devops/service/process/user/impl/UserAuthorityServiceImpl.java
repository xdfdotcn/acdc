package cn.xdf.acdc.devops.service.process.user.impl;

import cn.xdf.acdc.devops.core.domain.dto.UserAuthorityDTO;
import cn.xdf.acdc.devops.core.domain.entity.UserAuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserAuthorityQuery;
import cn.xdf.acdc.devops.repository.UserAuthorityRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.User;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class UserAuthorityServiceImpl implements UserAuthorityService {

    @Autowired
    private UserAuthorityRepository userAuthorityRepository;

    @Autowired
    private I18nService i18n;

    @Override
    @Transactional
    public List<UserAuthorityDTO> queryAll(final UserAuthorityQuery query) {
        return userAuthorityRepository.queryAll(query).stream()
                .map(UserAuthorityDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    public void addRole(final Long userId, final Set<AuthorityRoleType> roleSet) {
        if (CollectionUtils.isEmpty(roleSet)) {
            throw new ClientErrorException(i18n.msg(User.ERROR_ROLE, roleSet));
        }

        roleSet.add(AuthorityRoleType.ROLE_USER);
        Set<UserAuthorityDO> authoritySet = roleSet.stream()
                .map(it -> new UserAuthorityDO(userId, it))
                .collect(Collectors.toSet());
        List<UserAuthorityDO> authorityList = authoritySet.stream().collect(Collectors.toList());
        userAuthorityRepository.saveAll(authorityList);
    }

    @Override
    public void deleteRoleByUserId(final Long userId) {
        userAuthorityRepository.deleteRoleByUserId(userId);
    }

    @Override
    public void resetRole(final Long userId, final Set<AuthorityRoleType> roleSet) {
        deleteRoleByUserId(userId);
        addRole(userId, roleSet);
    }
}
