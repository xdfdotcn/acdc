package cn.xdf.acdc.devops.biz.connect.response;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class ConnectorStatusResponse {

    private static final String FAILED_STATUS = "failed";

    private static final String RUNNING_STATUS = "running";

    private static final String CONNECTOR_STATUS_KEY_STATE = "state";

    private static final String CONNECTOR_STATUS_KEY_TASK_ID = "id";

    private static final String CONNECTOR_STATUS_KEY_TASK_TRACE = "trace";

    private String name;

    private Map<String, String> connector;

    private List<Map<String, String>> tasks;

    private String type;

    private List<String> failedTasks = new ArrayList<>();

    private List<String> runningTasks = new ArrayList<>();

    private List<String> exceptions = new ArrayList<>();

    /**
     * Set failedTasks and runningTasks according to response tasks.
     *
     * @param tasks tasks
     */
    public void setTasks(final List<Map<String, String>> tasks) {
        this.tasks = tasks;
        if (tasks != null) {
            tasks.forEach(task -> {
                String state = task.get(CONNECTOR_STATUS_KEY_STATE);
                if (FAILED_STATUS.equalsIgnoreCase(state)) {
                    failedTasks.add(task.get(CONNECTOR_STATUS_KEY_TASK_ID));
                    exceptions.add(task.get(CONNECTOR_STATUS_KEY_TASK_TRACE));
                }
                if (RUNNING_STATUS.equalsIgnoreCase(state)) {
                    runningTasks.add(task.get(CONNECTOR_STATUS_KEY_TASK_ID));
                }
            });
        }
    }

    /**
     * Connector failed return true, else false.
     *
     * @return connector is failed or not
     */
    public boolean isConnectorFailed() {
        boolean isConnectorFailed = FAILED_STATUS.equalsIgnoreCase(connector.get(CONNECTOR_STATUS_KEY_STATE));
        boolean isAllTasksFailed = runningTasks.isEmpty() && !failedTasks.isEmpty();
        return isConnectorFailed || isAllTasksFailed;
    }

    /**
     * Get failed tasks ids.
     *
     * @return failed tasks ids
     */
    public List<String> getFailedTaskIds() {
        return failedTasks;
    }

}
