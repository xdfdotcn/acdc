package cn.xdf.acdc.devops.service.error.exceptions;

public class AcdcServiceException extends RuntimeException {

    public AcdcServiceException(final String message) {
        super(message);
    }

    public AcdcServiceException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public AcdcServiceException(final Throwable cause) {
        super(cause);
    }
}
