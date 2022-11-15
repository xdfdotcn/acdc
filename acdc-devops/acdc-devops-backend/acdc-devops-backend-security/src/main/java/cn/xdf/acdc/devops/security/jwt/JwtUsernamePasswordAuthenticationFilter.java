package cn.xdf.acdc.devops.security.jwt;

import cn.xdf.acdc.devops.security.util.SecurityUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Slf4j
public class JwtUsernamePasswordAuthenticationFilter extends AbstractAuthenticationProcessingFilter {

    private static final AntPathRequestMatcher DEFAULT_ANT_PATH_REQUEST_MATCHER = new AntPathRequestMatcher("/login",
            "POST");

    public JwtUsernamePasswordAuthenticationFilter() {
        super(DEFAULT_ANT_PATH_REQUEST_MATCHER);
    }

    /**
     * 从cookie 中提取用户名和密码,作为认证访问令牌.
     *
     * @param request  request
     * @param response response
     * @return Authentication Authentication
     * @throws AuthenticationException 认证失败异常
     */
    public Authentication attemptAuthentication(
            final HttpServletRequest request,
            final HttpServletResponse response
    ) throws AuthenticationException {
        String username = obtainUsername(request);
        String password = obtainPassword(request);
        log.info("Attempt authentication, username: {}, URL: {}", username, request.getRequestURI());

        UsernamePasswordAuthenticationToken authRequest = new UsernamePasswordAuthenticationToken(
                username, password);

        setDetails(request, authRequest);

        return this.getAuthenticationManager().authenticate(authRequest);
    }

    protected String obtainPassword(final HttpServletRequest request) throws AuthenticationException {
        return SecurityUtil.obtainPassword(request);
    }

    protected String obtainUsername(final HttpServletRequest request) {
        return SecurityUtil.obtainUsername(request);
    }

    protected void setDetails(final HttpServletRequest request,
            final UsernamePasswordAuthenticationToken authRequest) {
        authRequest.setDetails(authenticationDetailsSource.buildDetails(request));
    }
}
