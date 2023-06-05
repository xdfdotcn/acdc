package cn.xdf.acdc.devops.service.error.exceptions;

public class NotAuthorizedException extends ClientErrorException {
    
    public NotAuthorizedException(final String message) {
        super(message);
    }
    
    public NotAuthorizedException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public NotAuthorizedException(final Throwable cause) {
        super(cause);
    }
}
