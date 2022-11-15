package cn.xdf.acdc.devops.service.process.connection.approval.error;

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;

public class ApprovalProcessStateMatchErrorException extends AcdcServiceException {

    public ApprovalProcessStateMatchErrorException(final String message) {
        super(message);
    }

    public ApprovalProcessStateMatchErrorException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ApprovalProcessStateMatchErrorException(final Throwable cause) {
        super(cause);
    }
}
