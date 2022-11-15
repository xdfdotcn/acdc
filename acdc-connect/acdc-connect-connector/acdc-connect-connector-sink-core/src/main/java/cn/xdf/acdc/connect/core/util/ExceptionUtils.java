package cn.xdf.acdc.connect.core.util;

import com.google.common.base.Preconditions;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

public class ExceptionUtils {

    /**
     * Parse to flat message retriable exception.
     *
     * @param e exception
     * @return connect exception
     */
    public static RetriableException parseToFlatMessageRetriableException(final Exception e) {
        return new RetriableException(getFlatMessage(e), e);
    }

    /**
     * Parse to flat message connect exception.
     *
     * @param e exception
     * @return connect exception
     */
    public static ConnectException parseToFlatMessageConnectException(final Exception e) {
        return new ConnectException(getFlatMessage(e), e);
    }

    private static String getFlatMessage(final Exception e) {
        Preconditions.checkArgument(e instanceof Iterable, "input argument must be a instance of Iterable");

        StringBuilder message = new StringBuilder("Exception chain:").append(System.lineSeparator());
        Iterable<Throwable> iterater = (Iterable<Throwable>) e;
        for (Throwable each : iterater) {
            message.append(each).append(System.lineSeparator());
        }
        return message.toString();
    }

}
