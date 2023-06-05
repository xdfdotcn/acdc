package cn.xdf.acdc.devops.service.error;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import org.springframework.http.HttpStatus;

public class BadRequestException extends BizException {
    
    public BadRequestException(final String uiErrorMsg, final String innerErrorMsg) {
        super(HttpStatus.BAD_REQUEST, uiErrorMsg, innerErrorMsg);
    }
    
    public BadRequestException(final String innerErrorMsg) {
        super(HttpStatus.BAD_REQUEST, ErrorMsg.E_111, innerErrorMsg);
    }
    
    public BadRequestException() {
        super(HttpStatus.BAD_REQUEST, ErrorMsg.E_111, SystemConstant.EMPTY_STRING);
    }
}
