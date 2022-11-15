package cn.xdf.acdc.devops.informer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A informer get information from other system.
 *
 * @param <E> element
 */
public interface Informer<E> extends Runnable {

    /**
     * Wait until informer is initialized.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @throws InterruptedException interrupted exception
     * @throws TimeoutException timeout exception
     */
    void waitForInitialized(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * Is this informer initialized.
     *
     * @return is initialized or not
     */
    boolean isInitialized();

    /**
     * This informer's data.
     *
     * @return data
     */
    Collection<E> getAll();

    /**
     * Get data by key.
     * @param key key
     * @return element
     */
    E get(Long key);
}
