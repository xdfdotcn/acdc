package cn.xdf.acdc.devops.security.jwt.handler;

import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.security.jwt.JwtTokenProvider;
import cn.xdf.acdc.devops.security.jwt.ResponseWriter;
import cn.xdf.acdc.devops.security.jwt.LoginGuider;
import cn.xdf.acdc.devops.security.util.SecurityUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
public class JwtAuthenticationSuccessHandler implements AuthenticationSuccessHandler {

    @Autowired
    private JwtTokenProvider jwtTokenProvider;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void onAuthenticationSuccess(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final Authentication authentication) throws IOException, ServletException {
        LoginUserDTO userDetails = (LoginUserDTO) SecurityUtil.getCurrentUserDetails();
        String token = jwtTokenProvider.createToken(userDetails, Boolean.FALSE);
        ResponseWriter.write(response, objectMapper.writeValueAsString(new LoginGuider().setUser(userDetails).setToken(token)), HttpStatus.OK);

        log.info("Jwt authentication success, current user is: {}", userDetails);
    }
}
