package cn.xdf.acdc.devops.service.error;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * 业务异常.
 */
@Getter
public class BizException extends RuntimeException {

    private final HttpStatus status;

    private final String uiErrorMsg;

    public BizException(final HttpStatus status, final String uiErrorMsg, final String innerErrorMsg) {
        super(innerErrorMsg);
        this.status = status;
        this.uiErrorMsg = uiErrorMsg;
    }

    public BizException(final String message) {
        super(message);
        this.status = null;
        this.uiErrorMsg = null;
    }
}
