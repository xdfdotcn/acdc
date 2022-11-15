package cn.xdf.acdc.connector.tidb.reader;

import java.util.Map;

public interface TidbDataReader {

    /**
     * Do reading tidb data.
     */
    void doReading();

    /**
     * Close the reader.
     *
     * @throws InterruptedException interrupted exception
     */
    void close() throws InterruptedException;

    /**
     * Commit offset to the reader and allow reader commit to original source.
     *
     * @param partition partition
     * @param offset offset
     * @param orderInBatch event order in one batch
     */
    void markTicdcEventAsDone(int partition, long offset, int orderInBatch);

    /**
     * Register event listener.
     * @param listener event listener
     */
    void registerEventListener(EventListener listener);

    /**
     * Unregister event listener.
     */
    void unregisterEventListener();

    /**
     * Register runner lifecycle listener.
     *
     * @param listener runner lifecycle listener
     */
    void registerRunnerLifecycleListener(RunnerLifecycleListener listener);

    /**
     * Unregister runner lifecycle listener.
     */
    void unregisterRunnerLifecycleListener();

    /**
     * Get total reader runner.
     *
     * @return total reader runner
     */
    int getTotalRunnerCount();

    /**
     * Set total reader runner.
     *
     * @param totalRunnerCount total runner count
     */
    void setTotalRunnerCount(int totalRunnerCount);

    /**
     * Get alive reader runner count.
     *
     * @return alive reader runner count
     */
    int getAliveRunnerCount();

    /**
     * Get to commit offset.
     *
     * @return to commit offset
     */
    Map<Integer, Long> getToCommitOffset();
}
