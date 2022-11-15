package cn.xdf.acdc.devops.security.jwt.handler;

import cn.xdf.acdc.devops.security.jwt.ResponseWriter;
import cn.xdf.acdc.devops.security.jwt.LoginGuider;
import cn.xdf.acdc.devops.security.jwt.error.IllegalParameterAuthenticationException;
import cn.xdf.acdc.devops.security.jwt.error.InvalidUsernameOrPasswordAuthenticationException;
import cn.xdf.acdc.devops.security.util.SecurityUtil;
import cn.xdf.acdc.devops.service.util.UIError;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

@Slf4j
public class JwtAuthenticationFailureHandler implements AuthenticationFailureHandler {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private I18nService i18NService;

    @Override
    public void onAuthenticationFailure(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final AuthenticationException exception
    ) throws IOException {
        if (exception instanceof IllegalParameterAuthenticationException) {
            String loginUrl = SecurityUtil.generateLoginUrl(request);
            String content = objectMapper.writeValueAsString(LoginGuider.builder().url(loginUrl).build());
            ResponseWriter.write(response, content, HttpStatus.UNAUTHORIZED);
        } else if (exception instanceof InvalidUsernameOrPasswordAuthenticationException) {
            String message = i18NService.msg(I18nKey.Authorization.INVALID_USER);
            Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.PAYMENT_REQUIRED, message);
            String content = objectMapper.writeValueAsString(map);
            ResponseWriter.write(response, content, HttpStatus.PAYMENT_REQUIRED);
        } else {
            String message = i18NService.msg(I18nKey.SERVER_INTERNAL_ERROR);
            Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.INTERNAL_SERVER_ERROR, message);
            String content = objectMapper.writeValueAsString(map);
            ResponseWriter.write(response, content, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
