package cn.xdf.acdc.devops.security.util;

import cn.xdf.acdc.devops.security.jwt.error.IllegalParameterAuthenticationException;
import joptsimple.internal.Strings;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.util.Assert;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Utility class for Spring Security.
 */
public final class SecurityUtil {

    public static final String ANONYMOUS = "ROLE_ANONYMOUS";

    private static final String SPRING_SECURITY_FORM_USERNAME_KEY = "username";

    private static final String SPRING_SECURITY_FORM_PASSWORD_KEY = "password";

    private static final String LOGIN_URL_KEY = "loginUrl";

    private static final String LOGIN_SUCCESS_URL_KEY = "loginSuccessUrl";

    private static final String LOGOUT_SUCCESS_URL_KEY = "logoutSuccessUrl";

    private static final String DEFAULT_ROOT_PATH = "/";

    private static final String LOGIN_URL_FORMAT = "%s?loginSuccessUrl=%s";

    private SecurityUtil() {
    }

    /**
     * Get the login of the current user.
     *
     * @return the login of the current user.
     */
    public static Optional<String> getCurrentUserLogin() {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        return Optional.ofNullable(extractPrincipal(securityContext.getAuthentication()));
    }

    /**
     * Get the login of the current user's details.
     *
     * @return the login of the current user's details.
     */
    public static UserDetails getCurrentUserDetails() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Assert.notNull(authentication, "Error state, current user is unauthorized");
        return Optional.of((UserDetails) authentication.getDetails()).get();
    }

    private static String extractPrincipal(final Authentication authentication) {
        if (authentication == null) {
            return null;
        } else if (authentication.getPrincipal() instanceof UserDetails) {
            UserDetails springSecurityUser = (UserDetails) authentication.getPrincipal();
            return springSecurityUser.getUsername();
        } else if (authentication.getPrincipal() instanceof String) {
            return (String) authentication.getPrincipal();
        }
        return null;
    }

    /**
     * Check if a user is authenticated.
     *
     * @return true if the user is authenticated, false otherwise.
     */
    public static boolean isAuthenticated() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//    u   return authentication != null && !CollectionUtils.isEmpty(getAuthorities(authentication).collect(Collectors.toSet()));
        return authentication != null && getAuthorities(authentication).noneMatch(ANONYMOUS::equals);
    }

    /**
     * Checks if the current user has a specific authority.
     *
     * @param authority the authority to check.
     * @return true if the current user has the authority, false otherwise.
     */
    public static boolean hasCurrentUserThisAuthority(final String authority) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication != null && getAuthorities(authentication).anyMatch(authority::equals);
    }

    private static Stream<String> getAuthorities(final Authentication authentication) {
        return authentication.getAuthorities().stream().map(GrantedAuthority::getAuthority);
    }

    /**
     * Obtain password.
     *
     * @param request request
     * @return password
     * @throws AuthenticationException authentication failure Exception
     */
    public static String obtainPassword(final HttpServletRequest request) throws AuthenticationException {
        if (SecurityUtil.isAuthenticated()) {
            return SecurityUtil.getCurrentUserDetails().getPassword();
        }

        String password = request.getParameter(SPRING_SECURITY_FORM_PASSWORD_KEY);
        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalParameterAuthenticationException("The password must not be null.");
        }
        return password;
    }

    /**
     * Obtain username.
     *
     * @param request request
     * @return username
     * @throws AuthenticationException authentication failure Exception
     */
    public static String obtainUsername(final HttpServletRequest request) {
        if (SecurityUtil.isAuthenticated()) {
            return SecurityUtil.getCurrentUserDetails().getUsername();
        }

        String username = request.getParameter(SPRING_SECURITY_FORM_USERNAME_KEY);
        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalParameterAuthenticationException("The username must not be null.");
        }
        return username;
    }

    /**
     * Get login url.
     *
     * @param request request
     * @return login url
     */
    public static String generateLoginUrl(final HttpServletRequest request) {
        String loginUrl = obtainLoginUrl(request);
        String loginSuccessUrl = obtainLoginSuccessUrl(request);
        return String.format(LOGIN_URL_FORMAT, loginUrl, loginSuccessUrl);
    }

    /**
     * Get login url.
     *
     * @param request request
     * @return login url
     */
    public static String obtainLoginUrl(final HttpServletRequest request) {
        return getParameterWithDefaultValue(LOGIN_URL_KEY, request, DEFAULT_ROOT_PATH);
    }

    /**
     * Get login success url.
     *
     * @param request request
     * @return login success url
     */
    public static String obtainLoginSuccessUrl(final HttpServletRequest request) {
        return getParameterWithDefaultValue(LOGIN_SUCCESS_URL_KEY, request, DEFAULT_ROOT_PATH);
    }

    /**
     * Get logout success url.
     *
     * @param request request
     * @return login success url
     */
    public static String obtainLogoutSuccessUrl(final HttpServletRequest request) {
        return getParameterWithDefaultValue(LOGOUT_SUCCESS_URL_KEY, request, DEFAULT_ROOT_PATH);
    }

    private static String getParameterWithDefaultValue(final String key, final HttpServletRequest request, final String defaultValue) {
        String value = request.getHeader(key);
        return Strings.isNullOrEmpty(value) ? defaultValue : value;
    }
}
