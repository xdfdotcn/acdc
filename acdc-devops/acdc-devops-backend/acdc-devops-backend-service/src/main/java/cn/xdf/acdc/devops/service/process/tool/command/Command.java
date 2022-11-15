package cn.xdf.acdc.devops.service.process.tool.command;

import java.util.Map;

/**
 * Role (Receiver, Command).
 */
public interface Command<T> {
    /**
     * Execute a command.
     *
     * @param entity entity
     * @return result of execution, map
     */
    Map<String, Object> execute(T entity);
}
