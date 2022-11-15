package cn.xdf.acdc.connect.kafka.sink;

import lombok.Getter;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RecordForwardResult implements Future<Void> {

    @Getter
    private final long upstreamOffset;

    private volatile boolean isDone;

    private volatile Throwable throwable;

    public RecordForwardResult(final long upstreamOffset) {
        this.upstreamOffset = upstreamOffset;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Cancel operation is not supported.");
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException("Cancel operation is not supported.");
    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public Void get() {
        throw new UnsupportedOperationException("Please use get(long timeout, TimeUnit unit) instead.");
    }

    @Override
    public Void get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        synchronized (this) {
            if (!isDone()) {
                wait(unit.toMillis(timeout));
            }
        }

        if (!isDone()) {
            throw new TimeoutException("Timeout after waiting for " + timeout + unit);
        }

        if (throwable != null) {
            throw new ExecutionException(throwable);
        }

        return null;
    }

    /**
     * Notify this future is done.
     *
     * @param exception exception
     */
    public void done(final Exception exception) {
        this.throwable = exception;
        synchronized (this) {
            isDone = true;
            this.notify();
        }
    }
}
