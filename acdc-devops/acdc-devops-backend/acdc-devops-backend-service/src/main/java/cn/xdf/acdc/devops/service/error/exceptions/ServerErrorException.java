package cn.xdf.acdc.devops.service.error.exceptions;

public class ServerErrorException extends AcdcServiceException {
    
    public ServerErrorException(final String message) {
        super(message);
    }
    
    public ServerErrorException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public ServerErrorException(final Throwable cause) {
        super(cause);
    }
}
