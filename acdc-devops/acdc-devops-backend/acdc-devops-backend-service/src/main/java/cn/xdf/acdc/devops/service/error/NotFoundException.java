package cn.xdf.acdc.devops.service.error;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import org.springframework.http.HttpStatus;

public class NotFoundException extends BizException {

    public NotFoundException(final String uiErrorMsg, final String innerErrorMsg) {
        super(HttpStatus.NOT_FOUND, uiErrorMsg, innerErrorMsg);
    }

    public NotFoundException(final String innerErrorMsg) {
        super(HttpStatus.NOT_FOUND, ErrorMsg.E_101, innerErrorMsg);
    }

    public NotFoundException() {
        super(HttpStatus.NOT_FOUND, ErrorMsg.E_101, SystemConstant.EMPTY_STRING);
    }
}
