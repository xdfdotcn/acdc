package cn.xdf.acdc.devops.controller;

/**
 * A task, which can be start up, or stop.
 */
public interface Task {

    /**
     * Start up.
     */
    default void start() {
    }

    /**
     * Stop.
     */
    default void stop() {
    }
}
