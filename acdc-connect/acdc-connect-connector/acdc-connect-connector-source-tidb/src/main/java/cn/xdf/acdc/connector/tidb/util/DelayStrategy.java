package cn.xdf.acdc.connector.tidb.util;

import io.debezium.annotation.NotThreadSafe;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.function.BooleanSupplier;

@NotThreadSafe
@FunctionalInterface
public interface DelayStrategy {

    long INITIAL_DELAY_MILLISECONDS = 100L;

    // 10 minute
    long FORCE_WAKE_UP_INTERVAL_IN_MILLISECONDS = 10 * 60 * 1000;

    /**
     * Attempt to sleep when the specified criteria is met.
     *
     * @param criteria {@code true} if this method should sleep, or {@code false} if there is no need to sleep
     * @return {@code true} if this invocation caused the thread to sleep, or {@code false} if this method did not sleep
     */
    boolean sleepWhen(boolean criteria);

    /**
     * Attempt to sleep when the specified criteria is met.
     *
     * @param criteria {@code true} if this method should sleep, or {@code false} if there is no need to sleep
     */
    default void sleepWhenNotTimeout(BooleanSupplier criteria) {
        long startTime = System.currentTimeMillis();
        while (sleepWhen(criteria.getAsBoolean())) {
            long sleepTimeInMillis = System.currentTimeMillis() - startTime;
            if (sleepTimeInMillis > FORCE_WAKE_UP_INTERVAL_IN_MILLISECONDS) {
                throw new TimeoutException("force to wake up for having sleep " + sleepTimeInMillis + "ms");
            }
        }
    }

    /**
     * Create a delay strategy that applies an exponentially-increasing delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to zero.
     *
     * @param timeout the maximum delay; must be greater than the initial delay
     * @return the strategy; never null
     */
    static DelayStrategy exponentialWithTimeoutException(long timeout) {
        return exponentialWithTimeout(INITIAL_DELAY_MILLISECONDS, timeout, 2.0, true);
    }

    /**
     * Create a delay strategy that applies an exponentially-increasing delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to zero.Throw timeout exception if wait max delay time.
     *
     * @param initialDelayInMilliseconds the initial delay; must be positive
     * @param timeout                    the maximum delay; must be greater than the initial delay
     * @param backOffMultiplier          the factor by which the delay increases each pass
     * @param maybeThrowTimeoutException is allowed throw timeout exception
     * @return the strategy
     */
    static DelayStrategy exponentialWithTimeout(long initialDelayInMilliseconds, long timeout, double backOffMultiplier, boolean maybeThrowTimeoutException) {
        if (backOffMultiplier <= 1.0) {
            throw new IllegalArgumentException("Backup multiplier must be greater than 1");
        }
        if (initialDelayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        if (initialDelayInMilliseconds >= timeout) {
            throw new IllegalArgumentException("Maximum delay must be greater than initial delay");
        }
        return new DelayStrategyWithTimeoutException(initialDelayInMilliseconds, timeout, backOffMultiplier, maybeThrowTimeoutException);
    }

    /**
     * Create a delay strategy that applies a constant delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to zero.
     *
     * @param delayInMilliseconds the initial delay; must be positive
     * @return the strategy; never null
     */
    static DelayStrategy constant(long delayInMilliseconds) {
        return criteria -> {
            if (!criteria) {
                return false;
            }
            try {
                Thread.sleep(delayInMilliseconds);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        };
    }

    class DelayStrategyWithTimeoutException implements DelayStrategy {

        private final long initialDelayInMilliseconds;

        private final long timeout;

        private final double backOffMultiplier;

        private final boolean maybeThrowTimeoutException;

        private long previousDelay;

        private long leftDelay;

        public DelayStrategyWithTimeoutException(long initialDelayInMilliseconds, long timeout, double backOffMultiplier, boolean maybeThrowTimeoutException) {
            this.initialDelayInMilliseconds = initialDelayInMilliseconds;
            this.timeout = timeout;
            this.backOffMultiplier = backOffMultiplier;
            this.maybeThrowTimeoutException = maybeThrowTimeoutException;
            this.leftDelay = this.timeout;
        }

        @Override
        public boolean sleepWhen(final boolean criteria) {
            if (!criteria) {
                // Don't sleep ...
                previousDelay = 0;
                leftDelay = timeout;
                return false;
            }
            // Compute how long to delay ...
            if (previousDelay == 0) {
                previousDelay = initialDelayInMilliseconds;
            } else {
                long nextDelay = (long) (previousDelay * backOffMultiplier);
                previousDelay = Math.min(nextDelay, leftDelay);
            }

            if (leftDelay <= 0) {
                if (maybeThrowTimeoutException) {
                    throw new TimeoutException();
                }
                previousDelay = 0;
                leftDelay = timeout;
                return false;
            }

            // We expect to sleep ...
            try {
                Thread.sleep(previousDelay);
                leftDelay = leftDelay - previousDelay;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }
    }

}
