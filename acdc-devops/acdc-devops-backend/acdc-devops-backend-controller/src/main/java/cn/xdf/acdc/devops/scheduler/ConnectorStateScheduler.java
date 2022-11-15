package cn.xdf.acdc.devops.scheduler;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.service.entity.ConnectClusterService;
import cn.xdf.acdc.devops.statemachine.ConnectorStateHandler;
import cn.xdf.acdc.devops.statemachine.EventTriggerType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.micrometer.core.annotation.Timed;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ConnectorStateScheduler {

    private final TaskScheduler taskScheduler;

    private final ConnectClusterService connectClusterService;

    private final ConnectorStateHandler connectorStateHandler;

    @Value("${scheduler.user-trigger.interval.ms:10000}")
    private long userTriggerIntervalInMs;

    @Value("${scheduler.watch-cluster.interval.ms:20000}")
    private long watchClusterIntervalInMs;

    @Value("${scheduler.extend-event.interval.ms:10000}")
    private long innerExtendEventIntervalInMs;

    private final Map<Long, Map<EventTriggerType, List<ScheduledFuture<?>>>> schedulingTasks = new ConcurrentHashMap<>();

    private Set<Long> lastConnectClusterIds = new HashSet<>();

    /**
     * Construct a ConnectorStateScheduler instance.
     *
     * @param taskScheduler         taskScheduler
     * @param connectClusterService connectClusterService
     * @param connectorStateHandler connectorStateHandler
     */
    public ConnectorStateScheduler(final TaskScheduler taskScheduler, final ConnectClusterService connectClusterService,
        final ConnectorStateHandler connectorStateHandler) {
        this.taskScheduler = taskScheduler;
        this.connectClusterService = connectClusterService;
        this.connectorStateHandler = connectorStateHandler;
    }

    /**
     * Refresh to create new connect cluster tasks and remove deleted connect cluster tasks.
     */
    @Scheduled(fixedRateString = "${scheduler.refresh.scheduler-tasks.interval.ms:60000}")
    @Timed(description = "refresh scheduler tasks")
    public void refreshSchedulerTasks() {
        log.info("Refresh scheduler config...");
        Set<Long> currentConnectClusterIds = getCurrentConnectClusterIds();
        removeSchedulerTasks(currentConnectClusterIds);
        createSchedulerTasks(currentConnectClusterIds);
        lastConnectClusterIds = currentConnectClusterIds;
    }

    private void createSchedulerTasks(final Set<Long> currentConnectClusterIds) {
        Set<Long> toAdd = Sets.difference(currentConnectClusterIds, lastConnectClusterIds);

        toAdd.forEach(
            clusterId -> {
                Map<EventTriggerType, List<ScheduledFuture<?>>> eventMap = schedulingTasks.computeIfAbsent(clusterId, key -> new HashMap<>());
                // User trigger event executor
                connectorStateHandler.getUserTriggerEventHandlers().forEach((event, handler) -> {
                    List<ScheduledFuture<?>> scheduledFutures = eventMap.computeIfAbsent(EventTriggerType.USER, key -> Lists.newArrayList());
                    scheduledFutures.add(taskScheduler.scheduleAtFixedRate(() -> handler.accept(clusterId, event), Duration.ofMillis(userTriggerIntervalInMs)));
                });

                // watch connect cluster and update connect state
                ScheduledFuture<?> clusterScheduledFuture =
                    taskScheduler.scheduleAtFixedRate(() -> connectorStateHandler.connectClusterStateWatcher(clusterId), Duration.ofMillis(watchClusterIntervalInMs));
                eventMap.put(EventTriggerType.CONNECT_CLUSTER_WATCHER, Lists.newArrayList(clusterScheduledFuture));

                // watch inner extend event ,eg: timeout, retry, etc.
                ScheduledFuture<?> innerScheduledFuture =
                    taskScheduler.scheduleAtFixedRate(() -> connectorStateHandler.innerExtendEventWatcher(clusterId), Duration.ofMillis(innerExtendEventIntervalInMs));
                eventMap.put(EventTriggerType.INNER_EXTEND_EVENT_WATCHER, Lists.newArrayList(innerScheduledFuture));
            }
        );
    }

    private void removeSchedulerTasks(final Set<Long> currentConnectClusterIds) {
        Set<Long> toRemove = Sets.difference(lastConnectClusterIds, currentConnectClusterIds);
        toRemove.forEach(clusterId -> {
            schedulingTasks.get(clusterId).values().forEach(scheduledFutures -> scheduledFutures.forEach(scheduledFuture -> scheduledFuture.cancel(false)));
            schedulingTasks.remove(clusterId);
        });
    }

    private Set<Long> getCurrentConnectClusterIds() {
        List<ConnectClusterDO> connectClusterList = Optional.of(connectClusterService.findAll()).orElse(new ArrayList<>());
        return connectClusterList.stream().map(ConnectClusterDO::getId).collect(Collectors.toSet());
    }
}
