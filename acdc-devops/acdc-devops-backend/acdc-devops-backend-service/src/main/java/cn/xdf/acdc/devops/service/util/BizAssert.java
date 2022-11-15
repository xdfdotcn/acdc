package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.service.error.AlreadyExistsException;
import cn.xdf.acdc.devops.service.error.BadRequestException;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.exceptions.NotAuthorizedException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;

public final class BizAssert {

    /**
     * Not found resource.
     *
     * @param expression expression
     * @param uiErrorMsg ui error message
     * @param innerErrorMsg inner error message
     */
    public static void notFound(final boolean expression, final String uiErrorMsg, final String innerErrorMsg) {
        if (!expression) {
            throw new NotFoundException(uiErrorMsg, innerErrorMsg);
        }
    }

    /**
     * Not found resource.
     *
     * @param expression expression
     * @param innerErrorMsg inner error message
     */
    public static void notFound(final boolean expression, final String innerErrorMsg) {
        if (!expression) {
            throw new NotFoundException(innerErrorMsg);
        }
    }

    /**
     * Already exists resource.
     *
     * @param expression expression
     * @param uiErrorMsg ui error message
     * @param innerErrorMsg inner error message
     */
    public static void alreadyExists(final boolean expression, final String uiErrorMsg, final String innerErrorMsg) {
        if (!expression) {
            throw new AlreadyExistsException(uiErrorMsg, innerErrorMsg);
        }
    }

    /**
     * Inner error.
     *
     * @param expression expression
     * @param message inner error message
     */
    public static void innerError(final boolean expression, final String message) {
        if (!expression) {
            throw new ServerErrorException(message);
        }
    }

    /**
     * Bad request.
     *
     * @param expression expression
     * @param uiErrorMsg ui error message
     * @param innerErrorMsg inner error message
     */
    public static void badRequest(final boolean expression, final String uiErrorMsg, final String innerErrorMsg) {
        if (!expression) {
            throw new BadRequestException(uiErrorMsg, innerErrorMsg);
        }
    }

    /**
     * Bad request.
     *
     * @param uiErrorMsg ui error message
     * @param innerErrorMsg inner error message
     */
    public static void badRequest(final String uiErrorMsg, final String innerErrorMsg) {
        throw new BadRequestException(uiErrorMsg, innerErrorMsg);
    }

    /**
     * Not authorized.
     *
     * @param expression expression
     * @param message message
     */
    public static void notAuthorized(final boolean expression, final String message) {
        if (!expression) {
            throw new NotAuthorizedException(message);
        }
    }
}
