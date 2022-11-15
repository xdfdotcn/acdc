package cn.xdf.acdc.devops.statemachine;

import cn.xdf.acdc.devops.biz.connect.ConnectClusterRest;
import cn.xdf.acdc.devops.biz.connect.response.ConnectorStatusResponse;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorEvent;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.util.DelayStrategy;
import cn.xdf.acdc.devops.service.entity.ConnectClusterService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.jsonwebtoken.lang.Collections;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Slf4j
public class ConnectorStateHandler {

    // connector exec time out: 3 minutes
    public static final long CONNECTOR_EXEC_TIMEOUT_IN_MILLISECOND = 180_000L;

    private static final String GAUGE_METRICS_KEY_STATEMACHINE_NUM = "statemachine.num";

    private static final String GAUGE_METRICS_LABEL_STATE = "state";

    private static final long DELAY_STRATEGY_BASE_TIME_INTERVAL_IN_MILLISECOND = 30_000L;

    private static final long DELAY_STRATEGY_EXPIRE_TIME_IN_MILLISECOND = DelayStrategy.MAX_TIME_INTERVAL * 2;

    private final ConnectorCoreProcessService connectorCoreProcessService;

    private final ConnectClusterService connectClusterService;

    private final ConnectorStateMachineProvider connectorStateMachineProvider;

    private final ConnectClusterRest connectClusterRest;

    // connectId,ConnectorStateMachine tuple
    private final Map<Long, ConnectorStateMachine> stateMachineHolder = new ConcurrentHashMap<>();

    private final Map<UserTriggerConnectorEvent, EventHandleConsumer> userTriggerEventHandlers = new HashMap<>();

    private final Map<Long, Long> connectorExecStartTimeMap = new HashMap<>();

    private final Map<Long, DelayStrategy> connectorRetryDelayStrategyMap;

    private final MeterRegistry meterRegistry;

    private Long connectorExecTimeoutInMillisecond = CONNECTOR_EXEC_TIMEOUT_IN_MILLISECOND;

    /**
     * Construct a ConnectorStateHandler instance.
     *
     * @param connectorCoreProcessService     connectorCoreService
     * @param connectorStateMachineProvider connectorStateMachineProvider
     * @param connectClusterRest            connectClusterRest
     * @param connectClusterService         connectClusterService
     * @param meterRegistry                 meterRegistry
     */
    public ConnectorStateHandler(final ConnectorCoreProcessService connectorCoreProcessService, final ConnectorStateMachineProvider connectorStateMachineProvider,
            final ConnectClusterRest connectClusterRest, final ConnectClusterService connectClusterService, final MeterRegistry meterRegistry) {
        this.connectorCoreProcessService = connectorCoreProcessService;
        this.connectorStateMachineProvider = connectorStateMachineProvider;
        this.connectClusterRest = connectClusterRest;
        this.connectClusterService = connectClusterService;
        this.meterRegistry = meterRegistry;

        Cache<Long, DelayStrategy> connectorRetryDelayStrategyCache = CacheBuilder.newBuilder().expireAfterWrite(DELAY_STRATEGY_EXPIRE_TIME_IN_MILLISECOND, TimeUnit.MILLISECONDS).build();
        connectorRetryDelayStrategyMap = connectorRetryDelayStrategyCache.asMap();

        initDbEventHandlers();

        refreshStateMachineMetrics();
    }

    protected void setConnectorExecTimeoutInMillisecond(final Long timeout) {
        connectorExecTimeoutInMillisecond = timeout;
    }

    protected Map<Long, ConnectorStateMachine> getStateMachineHolder() {
        return stateMachineHolder;
    }

    private void refreshStateMachineMetrics() {
        Arrays.stream(ConnectorState.values()).forEach(
            state ->
                    Gauge.builder(GAUGE_METRICS_KEY_STATEMACHINE_NUM,
                            stateMachineHolder.values(), collection -> collection.stream().filter(stateMachine -> state.equals(stateMachine.getLastState())).count())
                            .tag(GAUGE_METRICS_LABEL_STATE, state.name())
                            .register(meterRegistry)
        );
    }

    private void initDbEventHandlers() {
        Arrays.stream(UserTriggerConnectorEvent.values()).forEach(
            userTriggerConnectorEvent -> userTriggerEventHandlers.put(userTriggerConnectorEvent, this::handleUserTriggerEvent)
        );
    }

    private void handleUserTriggerEvent(final Long clusterId, final UserTriggerConnectorEvent event) {
        List<ConnectorInfoDTO> connectorInfos = connectorCoreProcessService.queryConnector(event.getActual(), event.getDesired(), clusterId);
        if (!Collections.isEmpty(connectorInfos)) {
            List<Long> connectorIds = connectorInfos.stream().map(ConnectorInfoDTO::getId).collect(Collectors.toList());
            log.info("Begin to handle user trigger event: {}, cluster id: {}, connectorIds: {}.", event, clusterId, connectorIds);

            connectorInfos.forEach(connectorInfoDTO -> handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), event.getEvent()));

            log.info("End handle user trigger event: {}, cluster id: {}, connectorIds: {}.", event, clusterId, connectorIds);
        }
    }

    private void createConnectorStateMachineIfNotExist(final Long connectorId, final ConnectorState currentState) {
        if (!stateMachineHolder.containsKey(connectorId)) {
            stateMachineHolder.put(connectorId, connectorStateMachineProvider.getNewOne(currentState));
        }
    }

    private void handleEvent(final ConnectorInfoDTO connectorInfoDTO, final ConnectorState currentState, final ConnectorEvent event) {
        log.info("Begin to handle connector event: {}, connectorIds: {}, currentState: {}.", event, connectorInfoDTO.getId(), currentState);

        createConnectorStateMachineIfNotExist(connectorInfoDTO.getId(), currentState);
        ConnectorStateMachine connectorStateMachine = stateMachineHolder.get(connectorInfoDTO.getId());
        connectorStateMachine.fire(event, connectorInfoDTO);

        log.info("End handle connector event: {}, connectorIds: {}, currentState: {}.", event, connectorInfoDTO.getId(), currentState);
    }

    /**
     * Get user triggered events and handlers.
     *
     * @return user triggered events and handlers
     */
    public Map<UserTriggerConnectorEvent, EventHandleConsumer> getUserTriggerEventHandlers() {
        return userTriggerEventHandlers;
    }

    /**
     * Deal with event which connect cluster state change triggered.
     *
     * @param clusterId connect cluster id
     */
    @Timed(description = "watch connect cluster state")
    public void connectClusterStateWatcher(final Long clusterId) {
        // todo task restart
        Map<String, ConnectorStatusResponse> actualConnectorStatusMap = new HashMap<>();
        Map<String, Map<String, String>> actualConnectorConfigMap = new HashMap<>();

        ConnectClusterDO connectCluster = connectClusterService.findById(clusterId).get();
        String connectRestApiUrl = connectCluster.getConnectRestApiUrl();
        List<String> connectors = connectClusterRest.getAllConnectorByClusterUrl(connectRestApiUrl);
        // Get connector status and connector config from connect cluster
        connectors.forEach(connectorName -> {
            try {
                ConnectorStatusResponse connectorStatus = connectClusterRest.getConnectorStatus(connectRestApiUrl, connectorName);
                Map<String, String> connectorConfig = connectClusterRest.getConnectorConfig(connectRestApiUrl, connectorName);
                actualConnectorStatusMap.put(connectorName, connectorStatus);
                actualConnectorConfigMap.put(connectorName, connectorConfig);
            } catch (JsonProcessingException | ResourceAccessException e) {
                log.error("Connect to connect cluster error: connectRestApiUrl: {}, connectorName: {}, e: {}", connectRestApiUrl, connectorName, e);
                throw new RuntimeException(e);
            } catch (HttpClientErrorException e) {
                // unexpected exceptions
                if (!HttpStatus.NOT_FOUND.equals(e.getStatusCode())) {
                    throw e;
                }
            }
        });

        /**
         * produce event depend on actual state
         */
        // starting -> running
        getConnectorInfoFromDb(clusterId, ConnectorState.STARTING).forEach(connectorInfoDTO -> {
            if (actualConnectorStatusMap.containsKey(connectorInfoDTO.getName())) {
                ConnectorStatusResponse connectorStatus = actualConnectorStatusMap.get(connectorInfoDTO.getName());

                if (!handleFailedConnector(connectorStatus, connectorInfoDTO)) {
                    handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), ConnectorEvent.STARTUP_SUCCESS);
                }
            }
        });

        // running
        getConnectorInfoFromDb(clusterId, ConnectorState.RUNNING, ConnectorState.RUNNING).forEach(connectorInfoDTO -> {
            if (actualConnectorStatusMap.containsKey(connectorInfoDTO.getName())) {
                ConnectorStatusResponse connectorStatus = actualConnectorStatusMap.get(connectorInfoDTO.getName());

                if (!handleFailedConnector(connectorStatus, connectorInfoDTO)
                        && !connectorInfoDTO.getConnectorConfig().equals(actualConnectorConfigMap.get(connectorInfoDTO.getName()))) {

                    handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), ConnectorEvent.UPDATE);
                }
            }
        });

        // stopping -> stopped
        getConnectorInfoFromDb(clusterId, ConnectorState.STOPPING).forEach(connectorInfoDTO -> {
            if (!actualConnectorStatusMap.containsKey(connectorInfoDTO.getName())) {
                handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), ConnectorEvent.STOP_SUCCESS);
            }
        });
    }

    private boolean handleFailedConnector(final ConnectorStatusResponse connectorStatus, final ConnectorInfoDTO connectorInfoDTO) {
        if (connectorStatus.isConnectorFailed() || !connectorStatus.getFailedTaskIds().isEmpty()) {
            connectorInfoDTO.setRemark(connectorInfoDTO.getRemark() + connectorStatus.getExceptions().get(0));
            handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), ConnectorEvent.TASK_FAILURE);
            return true;
        }
        return false;
    }

    /**
     * Deal with inner or extend event.
     *
     * @param clusterId connect cluster id
     */
    @Timed(description = "watch inner extend event")
    public void innerExtendEventWatcher(final Long clusterId) {
        ConnectClusterDO connectCluster = connectClusterService.findById(clusterId).get();
        String connectRestApiUrl = connectCluster.getConnectRestApiUrl();
        List<String> connectors = connectClusterRest.getAllConnectorByClusterUrl(connectRestApiUrl);
        statusBack(clusterId, ConnectorState.STARTING, false, connectors);
        statusBack(clusterId, ConnectorState.STOPPING, true, connectors);
        updatingAutoToRunning(clusterId);

        // TODO pending -> creationFailed  resource limited
        validConnectorConfig(clusterId, connectRestApiUrl);

        retryRunningFailedConnectors(clusterId);

        reCreateConnectors(clusterId);
    }

    private void validConnectorConfig(final Long clusterId, final String connectRestApiUrl) {
        getConnectorInfoFromDb(clusterId, ConnectorState.PENDING).forEach(connectorInfoDTO -> {
            try {
                connectClusterRest.validConnectorConfig(connectRestApiUrl, connectorInfoDTO.getName(), connectorInfoDTO.getConnectorConfig());
            } catch (HttpServerErrorException exception) {
                log.error("Valid connector config error: {}", exception);
                connectorInfoDTO.setRemark(Arrays.toString(exception.getStackTrace()));
                handleEvent(connectorInfoDTO, ConnectorState.PENDING, ConnectorEvent.CREATE_FAILURE);
            }
        });
    }

    private void reCreateConnectors(final Long connectRestApiUrl) {
        List<ConnectorInfoDTO> connectorInfos = getConnectorInfoFromDb(connectRestApiUrl, ConnectorState.CREATION_FAILED, ConnectorState.RUNNING);
        connectorInfos.forEach(connectorInfoDTO -> {

            DelayStrategy delayStrategy = connectorRetryDelayStrategyMap.computeIfAbsent(connectorInfoDTO.getId(), key ->
                    new DelayStrategy(DELAY_STRATEGY_BASE_TIME_INTERVAL_IN_MILLISECOND));

            if (delayStrategy.isReached()) {
                handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), ConnectorEvent.RETRY);
            }
        });
    }

    private void retryRunningFailedConnectors(final Long clusterId) {
        List<ConnectorInfoDTO> connectorInfos = getConnectorInfoFromDb(clusterId, ConnectorState.RUNTIME_FAILED, ConnectorState.RUNNING);
        connectorInfos.forEach(connectorInfoDTO -> {

            DelayStrategy delayStrategy = connectorRetryDelayStrategyMap.computeIfAbsent(connectorInfoDTO.getId(), key ->
                    new DelayStrategy(DELAY_STRATEGY_BASE_TIME_INTERVAL_IN_MILLISECOND));

            if (delayStrategy.isReached()) {
                handleEvent(connectorInfoDTO, connectorInfoDTO.getActualState(), ConnectorEvent.RETRY);
            }
        });
    }

    private void updatingAutoToRunning(final Long clusterId) {
        getConnectorInfoFromDb(clusterId, ConnectorState.UPDATING).forEach(connectorInfoDTO -> {
            handleEvent(connectorInfoDTO, ConnectorState.UPDATING, ConnectorEvent.UPDATE_SUCCESS);
        });
    }

    private void statusBack(final Long clusterId, final ConnectorState connectorState, final boolean isExist, final List<String> connectors) {
        getConnectorInfoFromDb(clusterId, connectorState).forEach(connectorInfoDTO -> {
            if (isTimeOut(connectorInfoDTO) && isExist == connectors.contains(connectorInfoDTO.getName().trim())) {
                handleEvent(connectorInfoDTO, connectorState, ConnectorEvent.TIMEOUT);
                connectorExecStartTimeMap.remove(connectorInfoDTO.getId());
            }
        });
    }

    private boolean isTimeOut(final ConnectorInfoDTO connectorInfoDTO) {
        if (!connectorExecStartTimeMap.containsKey(connectorInfoDTO.getId())) {
            connectorExecStartTimeMap.put(connectorInfoDTO.getId(), System.currentTimeMillis());
            return false;
        }
        long execInterval = System.currentTimeMillis() - connectorExecStartTimeMap.get(connectorInfoDTO.getId());
        return execInterval > connectorExecTimeoutInMillisecond;
    }

    private List<ConnectorInfoDTO> getConnectorInfoFromDb(final Long connectClusterId, final ConnectorState currentState) {
        return connectorCoreProcessService.queryConnector(connectClusterId, currentState);
    }

    private List<ConnectorInfoDTO> getConnectorInfoFromDb(final Long connectClusterId, final ConnectorState currentState, final ConnectorState expectState) {
        List<ConnectorInfoDTO> connectorInfos = connectorCoreProcessService.queryConnector(currentState, expectState, connectClusterId);
        return connectorInfos == null ? new ArrayList<>() : connectorInfos;
    }

}
