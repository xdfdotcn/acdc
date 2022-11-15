package cn.xdf.acdc.devops.security.jwt.handler;

import cn.xdf.acdc.devops.security.jwt.ResponseWriter;
import cn.xdf.acdc.devops.security.jwt.LoginGuider;
import cn.xdf.acdc.devops.security.util.SecurityUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
public class JwtLogoutSuccessHandler implements LogoutSuccessHandler {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void onLogoutSuccess(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication) throws IOException, ServletException {

        String logoutSuccessUrl = SecurityUtil.obtainLogoutSuccessUrl(request);
        String content = objectMapper.writeValueAsString(LoginGuider.builder().url(logoutSuccessUrl).build());
        ResponseWriter.write(response, content, HttpStatus.UNAUTHORIZED);

        log.info("Logout success, logout success url is: {}", logoutSuccessUrl);
    }
}
