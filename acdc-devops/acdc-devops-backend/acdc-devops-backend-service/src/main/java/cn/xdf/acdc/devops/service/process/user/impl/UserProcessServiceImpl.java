package cn.xdf.acdc.devops.service.process.user.impl;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.query.UserAuthorityQuery;
import cn.xdf.acdc.devops.core.domain.query.UserQuery;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import cn.xdf.acdc.devops.service.util.UserUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class UserProcessServiceImpl implements UserProcessService {

    private static final String PERCENT = "%";

    private static final String ACTIVATED = "activated";

    private static final String LOGIN = "login";

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserAuthorityService userAuthorityService;

    @Override
    public UserDTO getUser(final Long id) {
        return userRepository.findById(id)
                .map(UserDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public UserDTO getUserByDomainAccount(final String domainAccount) {
        return userRepository.findOneByDomainAccountIgnoreCase(domainAccount)
                .map(UserDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("domainAccount: %s", domainAccount)));
    }

    @Override
    public boolean isAdmin(final String domainAccount) {
        UserDO user = userRepository.findOneByDomainAccountIgnoreCase(domainAccount)
                .orElseThrow(() -> new EntityNotFoundException(String.format("domainAccount: %s", domainAccount)));
        return UserUtil.isAdmin(user);
    }

    @Override
    public List<UserDTO> listUser(final String domainAccount) {
        //查询条件：where 'domainAccount' like %'?'%
        UserQuery query = UserQuery.builder().domainAccount(domainAccount).build();
        List<UserDO> doList = userRepository.query(query);
        List<UserDTO> dtoList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(doList)) {
            return dtoList;
        }
        return doList.stream()
                .map(UserDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    public List<UserDTO> query(final UserQuery query) {
        return userRepository.query(query).stream()
                .map(UserDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isDba(final String email) {
        UserAuthorityQuery userAuthorityQuery = UserAuthorityQuery.builder()
                .authorityRoleTypes(Sets.newHashSet(AuthorityRoleType.ROLE_DBA))
                .build();

        Set<Long> userIds = userAuthorityService.queryAll(userAuthorityQuery)
                .stream().map(it -> it.getUserId()).collect(Collectors.toSet());

        if (CollectionUtils.isEmpty(userIds)) {
            throw new ServerErrorException("User of the dba role, must not be empty,please check your config");
        }

        UserQuery userQuery = UserQuery.builder()
                .userIds(userIds)
                .build();

        return userRepository.query(userQuery).stream()
                .map(UserDO::getEmail)
                .collect(Collectors.toSet())
                .contains(email);
    }
}
