package cn.xdf.acdc.devops.security.jwt.error;

import org.springframework.security.authentication.InternalAuthenticationServiceException;

public class InvalidUsernameOrPasswordAuthenticationException extends InternalAuthenticationServiceException {

    public InvalidUsernameOrPasswordAuthenticationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidUsernameOrPasswordAuthenticationException(final String message) {
        super(message);
    }
}
