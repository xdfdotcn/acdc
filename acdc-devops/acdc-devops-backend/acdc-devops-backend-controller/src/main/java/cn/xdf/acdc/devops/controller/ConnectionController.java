package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.informer.AbstractInformer;
import cn.xdf.acdc.devops.informer.ConnectionInformer;
import cn.xdf.acdc.devops.informer.ConnectorInformer;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

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
@Component
@NotThreadSafe
@Slf4j
public class ConnectionController extends Controller {
    
    private static final int MAX_WAIT_TIME_IN_SECOND = 60;
    
    private final TaskScheduler taskScheduler;
    
    private final ConnectionService connectionService;
    
    private final ConnectorService connectorService;
    
    private final Map<Long, Set<Long>> connectorIdToConnectionId = new ConcurrentHashMap<>();
    
    private AbstractInformer<ConnectionDTO> connectionInformer;
    
    private AbstractInformer<ConnectorDTO> connectorInformer;
    
    public ConnectionController(final TaskScheduler taskScheduler, final ConnectionService connectionService,
                                final ConnectorService connectorService) {
        this.taskScheduler = taskScheduler;
        this.connectionService = connectionService;
        this.connectorService = connectorService;
    }
    
    @Override
    public void doStart() {
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
    
    @Override
    public void doStop() {
        connectorInformer.stop();
        connectionInformer.stop();
    }
    
    private void initConnectionInformer() {
        connectionInformer = new ConnectionInformer(taskScheduler, connectionService)
                .whenAdd(this::onConnectionAdd)
                .whenUpdate(this::onConnectionUpdate)
                .whenDelete(this::onConnectionDelete);
    }
    
    private void initConnectorInformer() {
        connectorInformer = new ConnectorInformer(taskScheduler, connectorService)
                .whenAdd(this::onConnectorActualStateChanged)
                .whenUpdate(this::onConnectorActualStateChanged);
    }
    
    /**
     * Connector state change will cause connection state updated, only response state change below witch means ignore other
     * state and these state change don't impact connection state before.
     * source connector desired state, source connector actual state, sink connector desired state,
     * sink connector actual state  =>  connection actual state
     * running, running, running, running  =>  running
     * any,     any,     stopped, stopped  =>  stopped
     * any,     any,     any,     failed   =>  failed
     * any,     failed,  any,     any      =>  failed
     *
     * @param connectorDto connector dto
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
            ConnectionDTO connectionDTO = waitForNonNull(() -> connectionInformer.get(connectionId));
            
            ConnectorDTO sourceConnectorDto;
            ConnectorDTO sinkConnectorDto;
            if (Objects.equals(connectionDTO.getSourceConnectorId(), connectorId)) {
                sourceConnectorDto = connectorDto;
                sinkConnectorDto = connectorInformer.get(connectionDTO.getSinkConnectorId());
            } else {
                sourceConnectorDto = connectorInformer.get(connectionDTO.getSourceConnectorId());
                sinkConnectorDto = connectorDto;
            }
            
            if (Objects.nonNull(sourceConnectorDto) && Objects.nonNull(sinkConnectorDto)) {
                Optional<ConnectionState> connectionState = generateConnectionState(sourceConnectorDto.getDesiredState(), sourceConnectorDto.getActualState(),
                        sinkConnectorDto.getDesiredState(), sinkConnectorDto.getActualState());
                connectionState.ifPresent(state -> refreshAndUpdateConnectionActualState(connectionDTO, state));
                
                // TODO: When deleting a connection, we should clean all the things in memory,
                // such as connector id to connection id mapping cache.
                // we should do the clean in the sink connector stopped/deleted event, but not connection.
                //                if (connectionDTO.getDeleted()) {
                //                    cleanConnectorIdToConnectionId(connectionDTO);
                //                }
            }
        });
    }
    
    private void cleanConnectorIdToConnectionId(final ConnectionDTO connectionDTO) {
        Long connectionId = connectionDTO.getId();
        
        Long sourceConnectorId = connectionDTO.getSourceConnectorId();
        if (sourceConnectorId != null) {
            Set<Long> connectionIds = connectorIdToConnectionId.get(sourceConnectorId);
            connectionIds.remove(connectionId);
            if (connectionIds.isEmpty()) {
                connectorIdToConnectionId.remove(sourceConnectorId);
            }
        }
        
        Long sinkConnectorId = connectionDTO.getSinkConnectorId();
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
    
    private void onConnectionAdd(final ConnectionDTO connectionDTO) {
        if (createConnectorIfNeeded(connectionDTO)) {
            return;
        }
        startOrStopConnectionIfNeeded(connectionDTO);
        mappingConnectorIdWithConnectionId(connectionDTO);
    }
    
    private void mappingConnectorIdWithConnectionId(final ConnectionDTO connectionDTO) {
        Long connectionId = connectionDTO.getId();
        Long sourceConnectorId = connectionDTO.getSourceConnectorId();
        Long sinkConnectorId = connectionDTO.getSinkConnectorId();
        
        // one source connector maybe source for many connection
        if (Objects.nonNull(sourceConnectorId)) {
            Set<Long> connectionIds = connectorIdToConnectionId.getOrDefault(sourceConnectorId, new HashSet<>());
            connectionIds.add(connectionId);
            connectorIdToConnectionId.put(sourceConnectorId, connectionIds);
        }
        
        if (Objects.nonNull(sinkConnectorId)) {
            connectorIdToConnectionId.computeIfAbsent(sinkConnectorId, key -> new HashSet<>()).add(connectionId);
        }
    }
    
    private boolean createConnectorIfNeeded(final ConnectionDTO connectionDTO) {
        if (!Objects.isNull(connectionDTO.getSinkConnectorId())) {
            return false;
        }
        
        if (ConnectionState.RUNNING.equals(connectionDTO.getDesiredState())) {
            refreshAndUpdateConnectionActualState(connectionDTO, ConnectionState.STARTING);
            
            ConnectionDTO appliedConnection = connectionService.applyConnectionToConnector(connectionDTO.getId());
            // refresh connectionDto in memory
            connectionDTO.setSinkConnectorId(appliedConnection.getSinkConnectorId());
            connectionDTO.setSourceConnectorId(appliedConnection.getSourceConnectorId());
            mappingConnectorIdWithConnectionId(connectionDTO);
        }
        return true;
    }
    
    private void onConnectionUpdate(final ConnectionDTO connectionDTO) {
        if (createConnectorIfNeeded(connectionDTO)) {
            return;
        }
        startOrStopConnectionIfNeeded(connectionDTO);
    }
    
    private void startOrStopConnectionIfNeeded(final ConnectionDTO connectionDTO) {
        if (!Objects.equals(connectionDTO.getActualState(), connectionDTO.getDesiredState())) {
            switch (connectionDTO.getDesiredState()) {
                case RUNNING:
                    startConnector(connectionDTO);
                    break;
                case STOPPED:
                    stopConnector(connectionDTO);
                    break;
                default:
                    break;
            }
        }
    }
    
    private void stopConnector(final ConnectionDTO connectionDTO) {
        refreshAndUpdateConnectionActualState(connectionDTO, ConnectionState.STOPPING);
        
        connectorService.stop(connectionDTO.getSinkConnectorId());
    }
    
    // todo check 页面更改配置时会同时更新connection和connector的配置
    private void startConnector(final ConnectionDTO connectionDTO) {
        refreshAndUpdateConnectionActualState(connectionDTO, ConnectionState.STARTING);
        
        connectorService.start(connectionDTO.getSinkConnectorId());
    }
    
    private void onConnectionDelete(final ConnectionDTO connectionDTO) {
        // stop connection if it is not actually stopped
        startOrStopConnectionIfNeeded(connectionDTO);
    }
    
    private void refreshAndUpdateConnectionActualState(final ConnectionDTO connectionDTO, final ConnectionState connectionState) {
        connectionService.updateActualState(connectionDTO.getId(), connectionState);
        // Refresh connection actual state in memory.
        connectionDTO.setActualState(connectionState);
    }
}
