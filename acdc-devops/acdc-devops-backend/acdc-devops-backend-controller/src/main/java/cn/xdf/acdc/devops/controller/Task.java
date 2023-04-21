package cn.xdf.acdc.devops.controller;

/**
 * A task, which can be start up, or stop.
 */
public interface Task {

    /**
     * Start up the task.
     */
    void start();

    /**
     * stop the task.
     */
    void stop();
}
