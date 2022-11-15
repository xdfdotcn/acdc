package cn.xdf.acdc.connector.tidb.reader;

public interface RunnerLifecycleListener {

    /**
     * Listening on runner start up.
     */
    void onStart();

    /**
     * Trigger stop by throw runtime exception or others.
     */
    void triggerStop();

    /**
     * Listening on runner stopped.
     */
    void onEnd();

}
