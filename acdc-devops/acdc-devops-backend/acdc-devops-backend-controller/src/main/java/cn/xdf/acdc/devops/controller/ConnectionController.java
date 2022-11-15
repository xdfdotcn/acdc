package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.informer.AbstractInformer;
import cn.xdf.acdc.devops.informer.ConnectionInformer;
import cn.xdf.acdc.devops.informer.ConnectorInformer;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorQueryProcessService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Connection controller controls data exchanges between connection and connector.
 */
@Service
@NotThreadSafe
@Slf4j
public class ConnectionController implements Controller {

    private static final int MAX_WAIT_TIME_IN_SECOND = 60;

    private final TaskScheduler taskScheduler;

    private final ConnectionProcessService connectionProcessService;

    private final ConnectorCoreProcessService connectorCoreProcessService;

    private final ConnectorQueryProcessService connectorQueryProcessService;

    private final Map<Long, Set<Long>> connectorIdToConnectionId = new ConcurrentHashMap<>();

    private AbstractInformer<ConnectionDetailDTO> connectionInformer;

    private AbstractInformer<ConnectorDTO> connectorInformer;

    public ConnectionController(final TaskScheduler taskScheduler, final ConnectionProcessService connectionProcessService,
            final ConnectorCoreProcessService connectorCoreProcessService, final ConnectorQueryProcessService connectorQueryProcessService) {
        this.taskScheduler = taskScheduler;
        this.connectionProcessService = connectionProcessService;
        this.connectorCoreProcessService = connectorCoreProcessService;
        this.connectorQueryProcessService = connectorQueryProcessService;
    }

    @PostConstruct
    @Override
    public void start() {
        initConnectionInformer();
        initConnectorInformer();

        connectionInformer.start();
        waitForConnectionInitialized();

        connectorInformer.start();
    }

    @SneakyThrows
    private void waitForConnectionInitialized() {
        connectionInformer.waitForInitialized(AbstractInformer.DEFAULT_INFORMER_INITIALIZATION_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    @Override
    public void stop() {
        connectorInformer.stop();
        connectionInformer.stop();
    }

    private void initConnectionInformer() {
        connectionInformer = new ConnectionInformer(taskScheduler, connectionProcessService)
                .whenAdd(this::onConnectionAdd)
                .whenUpdate(this::onConnectionUpdate)
                .whenDelete(this::onConnectionDelete);
    }

    private void initConnectorInformer() {
        connectorInformer = new ConnectorInformer(taskScheduler, connectorQueryProcessService)
                .whenAdd(this::onConnectorActualStateChanged)
                .whenUpdate(this::onConnectorActualStateChanged);
    }

    /**
     * Connector state change will cause connection state updated, only response state change below witch means ignore other
     *      state and these state change don't impact connection state before.
     * source connector desired state, source connector actual state, sink connector desired state,
     *      sink connector actual state  =>  connection actual state
     *    running, running, running, running  =>  running
     *    any,     any,     stopped, stopped  =>  stopped
     *    any,     any,     any,     failed   =>  failed
     *    any,     failed,  any,     any      =>  failed
     */
    private void onConnectorActualStateChanged(final ConnectorDTO connectorDto) {
        Long connectorId = connectorDto.getId();
        // WaitForNonNull help sink connector find the connection when connector first be created, while source connector can ignore.
        Set<Long> connectionIds = ConnectorType.SOURCE.equals(connectorDto.getConnectorType()) ? connectorIdToConnectionId.get(connectorId)
                : waitForNonNull(() -> connectorIdToConnectionId.get(connectorId));
        if (Objects.isNull(connectionIds)) {
            return;
        }

        connectionIds.forEach(connectionId -> {
            ConnectionDetailDTO connectionDetailDto = waitForNonNull(() -> connectionInformer.get(connectionId));

            ConnectorDTO sourceConnectorDto;
            ConnectorDTO sinkConnectorDto;
            if (Objects.equals(connectionDetailDto.getSourceConnectorId(), connectorId)) {
                sourceConnectorDto = connectorDto;
                sinkConnectorDto = connectorInformer.get(connectionDetailDto.getSinkConnectorId());
            } else {
                sourceConnectorDto = connectorInformer.get(connectionDetailDto.getSourceConnectorId());
                sinkConnectorDto = connectorDto;
            }

            if (Objects.nonNull(sourceConnectorDto) && Objects.nonNull(sinkConnectorDto)) {
                Optional<ConnectionState> connectionState = generateConnectionState(sourceConnectorDto.getDesiredState(), sourceConnectorDto.getActualState(),
                        sinkConnectorDto.getDesiredState(), sinkConnectorDto.getActualState());
                connectionState.ifPresent(state -> refreshAndUpdateConnectionActualState(connectionDetailDto, state));

                // TODO: When deleting a connection, we should clean all the things in memory,
                // such as connector id to connection id mapping cache.
                // we should do the clean in the sink connector stopped/deleted event, but not connection.
//                if (connectionDetailDto.getDeleted()) {
//                    cleanConnectorIdToConnectionId(connectionDetailDto);
//                }
            }
        });
    }

    private void cleanConnectorIdToConnectionId(final ConnectionDetailDTO connectionDetailDto) {
        Long connectionId = connectionDetailDto.getId();

        Long sourceConnectorId = connectionDetailDto.getSourceConnectorId();
        if (sourceConnectorId != null) {
            Set<Long> connectionIds = connectorIdToConnectionId.get(sourceConnectorId);
            connectionIds.remove(connectionId);
            if (connectionIds.isEmpty()) {
                connectorIdToConnectionId.remove(sourceConnectorId);
            }
        }

        Long sinkConnectorId = connectionDetailDto.getSinkConnectorId();
        if (sinkConnectorId != null) {
            connectorIdToConnectionId.remove(sinkConnectorId);
        }
    }

    @SneakyThrows
    private <T> T waitForNonNull(final Supplier<T> supplier) {
        int waitTimeInSeconds = 0;
        while (true) {
            T data = supplier.get();
            if (data != null) {
                return data;
            }

            TimeUnit.SECONDS.sleep(1);
            waitTimeInSeconds++;
            if (waitTimeInSeconds > MAX_WAIT_TIME_IN_SECOND) {
                throw new TimeoutException("Wait time out for " + waitTimeInSeconds + TimeUnit.SECONDS);
            }
        }
    }

    private Optional<ConnectionState> generateConnectionState(final ConnectorState sourceDesiredState, final ConnectorState sourceActualState,
            final ConnectorState sinkDesiredState, final ConnectorState sinkActualState) {
        if (ConnectorState.RUNNING.equals(sourceDesiredState) && ConnectorState.RUNNING.equals(sourceActualState)
                && ConnectorState.RUNNING.equals(sinkDesiredState) && ConnectorState.RUNNING.equals(sinkActualState)) {
            return Optional.of(ConnectionState.RUNNING);
        }

        if (ConnectorState.STOPPED.equals(sinkDesiredState)) {
            if (ConnectorState.STOPPED.equals(sinkActualState)) {
                return Optional.of(ConnectionState.STOPPED);
            }
            return Optional.empty();
        }

        if (ConnectorState.RUNTIME_FAILED.equals(sourceActualState) || ConnectorState.RUNTIME_FAILED.equals(sinkActualState)) {
            return Optional.of(ConnectionState.FAILED);
        }

        if (ConnectorState.CREATION_FAILED.equals(sourceActualState) || ConnectorState.CREATION_FAILED.equals(sinkActualState)) {
            return Optional.of(ConnectionState.FAILED);
        }
        return Optional.empty();
    }

    private void onConnectionAdd(final ConnectionDetailDTO connectionDetailDto) {
        if (createConnectorIfNeeded(connectionDetailDto)) {
            return;
        }
        startOrStopConnectionIfNeeded(connectionDetailDto);
        mappingConnectorIdWithConnectionId(connectionDetailDto);
    }

    private void mappingConnectorIdWithConnectionId(final ConnectionDetailDTO connectionDetailDto) {
        Long connectionId = connectionDetailDto.getId();
        Long sourceConnectorId = connectionDetailDto.getSourceConnectorId();
        Long sinkConnectorId = connectionDetailDto.getSinkConnectorId();

        // one source connector may mapping to many connection
        if (Objects.nonNull(sourceConnectorId)) {
            Set<Long> connectionIds = connectorIdToConnectionId.getOrDefault(sourceConnectorId, new HashSet<>());
            connectionIds.add(connectionId);
            connectorIdToConnectionId.put(sourceConnectorId, connectionIds);
        }

        if (Objects.nonNull(sinkConnectorId)) {
            connectorIdToConnectionId.computeIfAbsent(sinkConnectorId, key -> new HashSet<>()).add(connectionId);
        }
    }

    private boolean createConnectorIfNeeded(final ConnectionDetailDTO connectionDetailDto) {
        if (!Objects.isNull(connectionDetailDto.getSinkConnectorId())) {
            return false;
        }

        if (ConnectionState.RUNNING.equals(connectionDetailDto.getDesiredState())) {
            refreshAndUpdateConnectionActualState(connectionDetailDto, ConnectionState.STARTING);

            ConnectionDetailDTO appliedConnection = connectionProcessService.applyConnectionToConnector(connectionDetailDto);

            // refresh connectionDto in memory
            connectionDetailDto.setSinkConnectorId(appliedConnection.getSinkConnectorId());
            connectionDetailDto.setSourceConnectorId(appliedConnection.getSourceConnectorId());
            mappingConnectorIdWithConnectionId(connectionDetailDto);
        }
        return true;
    }

    private void onConnectionUpdate(final ConnectionDetailDTO connectionDetailDto) {
        if (createConnectorIfNeeded(connectionDetailDto)) {
            return;
        }
        startOrStopConnectionIfNeeded(connectionDetailDto);
    }

    private void startOrStopConnectionIfNeeded(final ConnectionDetailDTO connectionDetailDto) {
        if (!Objects.equals(connectionDetailDto.getActualState(), connectionDetailDto.getDesiredState())) {
            switch (connectionDetailDto.getDesiredState()) {
                case RUNNING:
                    startConnector(connectionDetailDto);
                    break;
                case STOPPED:
                    stopConnector(connectionDetailDto);
                    break;
                default:
                    break;
            }
        }
    }

    private void stopConnector(final ConnectionDetailDTO connectionDetailDto) {
        refreshAndUpdateConnectionActualState(connectionDetailDto, ConnectionState.STOPPING);

        updateConnectorDesiredState(connectionDetailDto.getSinkConnectorId(), ConnectorState.STOPPED);
    }

    private void startConnector(final ConnectionDetailDTO connectionDetailDto) {
        refreshAndUpdateConnectionActualState(connectionDetailDto, ConnectionState.STARTING);

        flushConnectionConfigToConnector(connectionDetailDto);
        updateConnectorDesiredState(connectionDetailDto.getSinkConnectorId(), ConnectorState.RUNNING);
    }

    private void updateConnectorDesiredState(final Long connectorId, final ConnectorState state) {
        connectorCoreProcessService.updateDesiredState(connectorId, state);
    }

    private void flushConnectionConfigToConnector(final ConnectionDetailDTO connectionDetailDto) {
        connectionProcessService.flushConnectionConfigToConnector(connectionDetailDto);
    }

    private void onConnectionDelete(final ConnectionDetailDTO connectionDetailDto) {
        // stop connection if it is not actually stopped
        startOrStopConnectionIfNeeded(connectionDetailDto);
    }

    private void refreshAndUpdateConnectionActualState(final ConnectionDetailDTO connectionDetailDto, final ConnectionState connectionState) {
        connectionProcessService.editActualState(connectionDetailDto.getId(), connectionState);
        // Refresh connection actual state in memory.
        connectionDetailDto.setActualState(connectionState);
    }
}
