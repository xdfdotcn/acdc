package cn.xdf.acdc.devops.security.jwt;

import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.User;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;

/**
 * Authenticate a user from the database.
 */
@Service
@Transactional
@Slf4j
public class UserDetailsServiceImpl implements UserDetailsService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private I18nService i18n;

    @Override
    public UserDetails loadUserByUsername(final String email) {
        log.info("Prepare load user, email is: {}", email);

        UserDO userDO = userRepository.findOneByDomainAccountIgnoreCase(email)
                .orElseGet(() -> userRepository.findOneByEmailIgnoreCase(email)
                        .orElseThrow(() -> new EntityNotFoundException(i18n.msg(User.NOT_FOUND, email)))
                );

        return new LoginUserDTO(userDO);
    }
}
