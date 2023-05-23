package cn.xdf.acdc.devops.service.error.exceptions;

public class ClientErrorException extends AcdcServiceException {
    
    public ClientErrorException(final String message) {
        super(message);
    }
    
    public ClientErrorException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public ClientErrorException(final Throwable cause) {
        super(cause);
    }
}
