package cn.xdf.acdc.devops.security.jwt;

import cn.xdf.acdc.devops.security.jwt.error.InvalidUsernameOrPasswordAuthenticationException;
import cn.xdf.acdc.devops.security.util.SecurityUtil;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;

import javax.persistence.EntityNotFoundException;
import java.util.Objects;

@Slf4j
public class JwtAuthenticationProvider implements AuthenticationProvider {

    @Autowired
    private UserDetailsService userDetailsService;

    @Override
    public Authentication authenticate(final Authentication authentication) throws AuthenticationException {
        try {
            log.info("Prepare authenticate, principal: {}", authentication.getPrincipal());

            String userName = String.valueOf(authentication.getPrincipal());
            String password = String.valueOf(authentication.getCredentials());

            if (SecurityUtil.isAuthenticated()) {
                log.info("The user has logged in to the system. The authentication is cancelled.");
                return newUsernamePasswordAuthenticationToken(SecurityUtil.getCurrentUserDetails());
            }

            UserDetails userDetails = userDetailsService.loadUserByUsername(userName);
            if (Objects.isNull(userDetails)
                    || !Objects.equals(password, EncryptUtil.decrypt(userDetails.getPassword()))
            ) {
                throw new InvalidUsernameOrPasswordAuthenticationException("Invalid username or password, username: " + userName);
            }

            return newUsernamePasswordAuthenticationToken(userDetails);
        } catch (EntityNotFoundException e) {
            throw new InvalidUsernameOrPasswordAuthenticationException(e.getMessage(), e.getCause());
        }
    }

    private UsernamePasswordAuthenticationToken newUsernamePasswordAuthenticationToken(final UserDetails userDetails) {
        UsernamePasswordAuthenticationToken usernamePasswordAuthenticationToken =
                new UsernamePasswordAuthenticationToken(
                        userDetails.getUsername(),
                        userDetails.getPassword(),
                        userDetails.getAuthorities()
                );
        usernamePasswordAuthenticationToken.setDetails(userDetails);

        return usernamePasswordAuthenticationToken;
    }

    @Override
    public boolean supports(final Class<?> aClass) {
        return true;
    }
}
