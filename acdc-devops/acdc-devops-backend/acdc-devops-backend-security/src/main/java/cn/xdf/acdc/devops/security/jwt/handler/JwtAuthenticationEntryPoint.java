package cn.xdf.acdc.devops.security.jwt.handler;

import cn.xdf.acdc.devops.security.jwt.ResponseWriter;
import cn.xdf.acdc.devops.security.jwt.LoginGuider;
import cn.xdf.acdc.devops.security.util.SecurityUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
public class JwtAuthenticationEntryPoint implements AuthenticationEntryPoint {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void commence(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final AuthenticationException authException
    ) throws IOException {
        String loginUrl = SecurityUtil.generateLoginUrl(request);
        String content = objectMapper.writeValueAsString(LoginGuider.builder().url(loginUrl).build());
        ResponseWriter.write(response, content, HttpStatus.UNAUTHORIZED);

        log.info("Current user unauthorized, leading the user to login, login url is: {}", loginUrl);
    }
}
