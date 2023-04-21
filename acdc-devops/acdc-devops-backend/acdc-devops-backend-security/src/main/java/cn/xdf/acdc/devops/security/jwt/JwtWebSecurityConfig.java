package cn.xdf.acdc.devops.security.jwt;

import cn.xdf.acdc.devops.security.jwt.handler.JwtAuthenticationEntryPoint;
import cn.xdf.acdc.devops.security.jwt.handler.JwtAuthenticationFailureHandler;
import cn.xdf.acdc.devops.security.jwt.handler.JwtAuthenticationSuccessHandler;
import cn.xdf.acdc.devops.security.jwt.handler.JwtLogoutSuccessHandler;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.logout.LogoutFilter;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.web.filter.GenericFilterBean;

import java.util.ArrayList;
import java.util.List;

/**
 * JWT 集成 spring security. Security 拦截器加载顺序参考:{@link org.springframework.security.config.annotation.web.HttpSecurityBuilder}
 */
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true)
@ConfigurationProperties(prefix = "acdc.api.security")
public class JwtWebSecurityConfig extends WebSecurityConfigurerAdapter {

    @Setter
    private List<String> uriWhitelist = new ArrayList<>();

    @Override
    protected void configure(final HttpSecurity http) throws Exception {
        http
                .addFilterAt(usernamePasswordAuthenticationFilter(), UsernamePasswordAuthenticationFilter.class)
                .addFilterAt(getJwtLogoutFilter(), LogoutFilter.class)
                .addFilterBefore(jwtFilter(), UsernamePasswordAuthenticationFilter.class)
                .authorizeRequests()
                .and()
                .csrf().disable().exceptionHandling().authenticationEntryPoint(jwtLoginEntryPoint())
                .and()
                .sessionManagement()
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .authorizeRequests()
                .antMatchers(uriWhitelist.toArray(new String[0])).permitAll()
                .anyRequest()
                .authenticated();
    }

    /**
     * 权限认证提供者配置.
     *
     * @param auth auth
     */
    @Override
    protected void configure(final AuthenticationManagerBuilder auth) {
        auth.authenticationProvider(authenticationProvider());
    }

    /**
     * 登录成功处理器.
     *
     * @return JwtLoginSuccessHandler
     */
    @Bean
    public JwtAuthenticationSuccessHandler jwtLoginSuccessHandler() {
        return new JwtAuthenticationSuccessHandler();
    }

    /**
     * 登录失败处理器.
     *
     * @return JwtLoginFailureHandler
     */
    @Bean
    public JwtAuthenticationFailureHandler jwtLoginFailureHandler() {
        return new JwtAuthenticationFailureHandler();
    }

    /**
     * 未授权异常处理处理器.
     *
     * @return JwtLoginEntryPoint
     */
    @Bean
    public JwtAuthenticationEntryPoint jwtLoginEntryPoint() {
        return new JwtAuthenticationEntryPoint();
    }


    /**
     * JWT token 提供者.
     *
     * @return JwtTokenProvider
     */
    public JwtTokenProvider jwtTokenProvider() {
        return new JwtTokenProvider();
    }

    /**
     * JWT token 过滤器.
     *
     * @return JwtFilter
     */
    public JwtFilter jwtFilter() {
        return new JwtFilter(jwtTokenProvider());
    }

    /**
     * 提取用户名密码过滤器.
     *
     * @return JwtUsernamePasswordAuthenticationFilter
     * @throws Exception 配置错误
     */
    @Bean
    public JwtUsernamePasswordAuthenticationFilter usernamePasswordAuthenticationFilter() throws Exception {
        JwtUsernamePasswordAuthenticationFilter filter = new JwtUsernamePasswordAuthenticationFilter();

        filter.setAuthenticationManager(authenticationManagerBean());
        filter.setAuthenticationSuccessHandler(jwtLoginSuccessHandler());
        filter.setAuthenticationFailureHandler(jwtLoginFailureHandler());

        return filter;
    }

    /**
     * 权限认证管理者.
     *
     * @return AuthenticationManager
     */
    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }

    /**
     * 自定义权限认证提供者.
     *
     * @return JwtAuthenticationProvider
     */
    @Bean
    public AuthenticationProvider authenticationProvider() {
        JwtAuthenticationProvider provider = new JwtAuthenticationProvider();
        return provider;
    }

    /**
     * 登出过滤器 .
     *
     * @return JwtLogoutFilter
     */
    @Bean
    public GenericFilterBean getJwtLogoutFilter() {
        return new JwtLogoutFilter(getJwtLogoutSuccessHandler(), new SecurityContextLogoutHandler());
    }

    /**
     * 登出成功处理器.
     *
     * @return JwtLogoutSuccessHandler
     */
    @Bean
    public JwtLogoutSuccessHandler getJwtLogoutSuccessHandler() {
        return new JwtLogoutSuccessHandler();
    }
}
