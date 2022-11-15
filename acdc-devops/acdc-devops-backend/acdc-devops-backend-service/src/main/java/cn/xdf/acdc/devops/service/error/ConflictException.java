package cn.xdf.acdc.devops.service.error;

import org.springframework.http.HttpStatus;

public class ConflictException extends BizException {

    public ConflictException(final String uiErrorMsg) {
        super(HttpStatus.CONFLICT, uiErrorMsg, uiErrorMsg);
    }
}
