package cn.xdf.acdc.devops.security.jwt.error;

import org.springframework.security.authentication.InternalAuthenticationServiceException;

public class IllegalParameterAuthenticationException extends InternalAuthenticationServiceException {

    public IllegalParameterAuthenticationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IllegalParameterAuthenticationException(final String message) {
        super(message);
    }
}
