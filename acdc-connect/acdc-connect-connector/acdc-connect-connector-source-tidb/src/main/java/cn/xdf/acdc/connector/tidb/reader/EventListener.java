package cn.xdf.acdc.connector.tidb.reader;

public interface EventListener {

    /**
     * Listening on event arrived.
     *
     * @param event event
     */
    void onEvent(Event event);

}
