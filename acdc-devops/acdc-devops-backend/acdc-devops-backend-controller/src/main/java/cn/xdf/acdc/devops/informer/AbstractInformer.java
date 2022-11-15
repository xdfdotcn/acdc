package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.controller.FixedRateRunnableTask;
import io.jsonwebtoken.lang.Collections;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Abstract informer provide common process capabilities.
 *
 * @param <E> element
 */
public abstract class AbstractInformer<E> extends FixedRateRunnableTask implements Informer<E> {

    public static final int DEFAULT_INFORMER_INITIALIZATION_TIMEOUT_IN_SECONDS = 300;

    private final Object initializationBlocker = new Object();

    private Map<Long, E> resources;

    private Instant lastUpdateTime;

    private List<Consumer<E>> addCallbacks;

    private List<Consumer<E>> updateCallbacks;

    private List<Consumer<E>> deleteCallbacks;

    private volatile boolean isInitialized;

    public AbstractInformer(final TaskScheduler scheduler) {
        super(scheduler);

        lastUpdateTime = Instant.ofEpochSecond(0);
        resources = new HashMap<>();
        addCallbacks = new ArrayList<>();
        updateCallbacks = new ArrayList<>();
        deleteCallbacks = new ArrayList<>();
    }

    @Override
    public void run() {
        List<E> dataList = query();

        final AtomicReference<Instant> tmpUpdateTime = new AtomicReference<>(lastUpdateTime);
        if (!Collections.isEmpty(dataList)) {
            dataList.forEach(newer ->
                    resources.compute(getKey(newer), (key, order) -> {
                        invokeAppropriateCallbacks(order, newer);

                        Instant updateTime = getUpdateTime(newer);
                        if (tmpUpdateTime.get().compareTo(updateTime) < 0) {
                            tmpUpdateTime.set(updateTime);
                        }
                        return newer;
                    })
            );
        }

        lastUpdateTime = tmpUpdateTime.get();

        if (!isInitialized) {
            synchronized (initializationBlocker) {
                isInitialized = true;
                initializationBlocker.notify();
            }
        }
    }

    @Override
    public void waitForInitialized(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        synchronized (initializationBlocker) {
            if (!isInitialized) {
                initializationBlocker.wait(unit.toMillis(timeout));
            }
        }
        if (!isInitialized) {
            throw new TimeoutException("Timeout after waiting for " + timeout + unit);
        }
    }

    @Override
    public boolean isInitialized() {
        return isInitialized;
    }

    private void invokeAppropriateCallbacks(final E order, final E newer) {
        if (order == null) {
            addCallbacks.forEach(consumer -> consumer.accept(newer));
        } else if (isDeleted(order, newer)) {
            deleteCallbacks.forEach(consumer -> consumer.accept(newer));
        } else if (!equals(newer, order)) {
            updateCallbacks.forEach(consumer -> consumer.accept(newer));
        }
    }

    /**
     * Register add call back.
     *
     * @param callback call back
     * @return this informer
     */
    public AbstractInformer<E> whenAdd(final Consumer<E> callback) {
        addCallbacks.add(callback);
        return this;
    }

    /**
     * Register delete call back.
     *
     * @param callback call back
     * @return this informer
     */
    public AbstractInformer<E> whenDelete(final Consumer<E> callback) {
        deleteCallbacks.add(callback);
        return this;
    }

    /**
     * Register update call back.
     *
     * @param callback call back
     * @return this informer
     */
    public AbstractInformer<E> whenUpdate(final Consumer<E> callback) {
        updateCallbacks.add(callback);
        return this;
    }

    @Override
    public E get(final Long key) {
        return resources.get(key);
    }

    @Override
    public Collection<E> getAll() {
        return resources.values();
    }

    /**
     * Informer detects information from other system.
     *
     * @return data
     */
    abstract List<E> query();

    /**
     * Element's key.
     *
     * @param element element
     * @return key
     */
    abstract Long getKey(E element);

    /**
     * If the two is equal.
     *
     * @param e1 element 1
     * @param e2 element 2
     * @return is equal or not
     */
    abstract boolean equals(E e1, E e2);

    /**
     * Is this record is deleted.
     *
     * @param older order object
     * @param newer newer object
     * @return is deleted or not
     */
    abstract boolean isDeleted(E older, E newer);

    /**
     * Get element's update time.
     *
     * @param e element
     * @return update time
     */
    abstract Instant getUpdateTime(E e);

    Instant getLastUpdateTime() {
        return this.lastUpdateTime;
    }
}
