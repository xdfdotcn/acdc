package cn.xdf.acdc.devops.service.error;

import org.springframework.http.HttpStatus;

public class AlreadyExistsException extends BizException {

    public AlreadyExistsException(final String uiErrorMsg, final String innerErrorMsg) {
        super(HttpStatus.CONFLICT, uiErrorMsg, innerErrorMsg);
    }
}
