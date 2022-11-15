package cn.xdf.acdc.devops.service.error;

public class SystemBizException extends RuntimeException {

    public SystemBizException(final String message) {
        super(message);
    }

    public SystemBizException(final Throwable cause) {
        super(cause);
    }
}
