package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import cn.xdf.acdc.devops.informer.AbstractFixedRateRunnableInformer;
import cn.xdf.acdc.devops.informer.DataSystemResourcePermissionRequisitionBatchInformer;
import cn.xdf.acdc.devops.informer.WideTableInformer;
import cn.xdf.acdc.devops.informer.ConnectionInformer;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import cn.xdf.acdc.devops.service.process.widetable.WideTableService;
import io.jsonwebtoken.lang.Collections;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class WideTableController extends Controller {
    
    private final WideTableService wideTableService;
    
    private final AbstractFixedRateRunnableInformer<WideTableDTO> wideTableInformer;
    
    private final AbstractFixedRateRunnableInformer<ConnectionDTO> connectionInformer;
    
    private final AbstractFixedRateRunnableInformer<DataSystemResourcePermissionRequisitionBatchDTO> requisitionBatchInformer;
    
    private final LinkedBlockingQueue<Runnable> events = new LinkedBlockingQueue<>();
    
    private final Map<Long, Set<Long>> connectionIdToWideTableIds = new HashMap<>();
    
    private final Map<Long, Long> requisitionBatchIdToWideTableIds = new HashMap<>();
    
    private volatile boolean enableRunning = true;
    
    public WideTableController(final WideTableService wideTableService,
                               final ConnectionService connectionService,
                               final DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService,
                               final TaskScheduler taskScheduler) {
        this.wideTableInformer = new WideTableInformer(taskScheduler, wideTableService)
                .whenAdd(this::addWideTableEventToTheQueue)
                .whenUpdate(this::addWideTableEventToTheQueue);
        this.connectionInformer = new ConnectionInformer(taskScheduler, connectionService)
                .whenAdd(this::addConnectionEventToTheQueue)
                .whenUpdate(this::addConnectionEventToTheQueue);
        this.requisitionBatchInformer = new DataSystemResourcePermissionRequisitionBatchInformer(taskScheduler, dataSystemResourcePermissionRequisitionBatchService)
                .whenAdd(this::addRequisitionEventToTheQueue)
                .whenUpdate(this::addRequisitionEventToTheQueue);
        this.wideTableService = wideTableService;
    }
    
    private void addRequisitionEventToTheQueue(final DataSystemResourcePermissionRequisitionBatchDTO dataSystemResourcePermissionRequisitionBatchDTO) {
        events.add(() -> handleRequisitionBatchEvent(dataSystemResourcePermissionRequisitionBatchDTO));
    }
    
    private void handleRequisitionBatchEvent(final DataSystemResourcePermissionRequisitionBatchDTO dataSystemResourcePermissionRequisitionBatchDTO) {
        Long requisitionBatchId = dataSystemResourcePermissionRequisitionBatchDTO.getId();
        Long wideTableId = requisitionBatchIdToWideTableIds.get(requisitionBatchId);
        if (wideTableId == null) {
            return;
        }
        WideTableDTO wideTableDTO = wideTableInformer.get(wideTableId);
        if (wideTableDTO == null) {
            return;
        }
        updateWideTableRequisitionStateIfNeeded(dataSystemResourcePermissionRequisitionBatchDTO.getState(), wideTableId, wideTableDTO.getRequisitionState());
    }
    
    private void updateWideTableRequisitionStateIfNeeded(final ApprovalBatchState requisitionBatchState,
                                                         final Long wideTableId, final RequisitionState currentWideTableRequisitionState) {
        if (currentWideTableRequisitionState == RequisitionState.APPROVING) {
            switch (requisitionBatchState) {
                case REFUSED:
                    wideTableService.updateRequisitionStateToRefused(wideTableId);
                    break;
                case APPROVED:
                    wideTableService.createDataSystemResourceAndUpdateWideTable(wideTableId);
                    break;
                default:
                    break;
            }
        }
    }
    
    private void addConnectionEventToTheQueue(final ConnectionDTO connectionDTO) {
        events.add(() -> handleConnectionEvent(connectionDTO));
    }
    
    private void handleConnectionEvent(final ConnectionDTO connectionDTO) {
        updateWideTableActualStateIfNeeded(connectionDTO.getId());
    }
    
    private void updateWideTableActualStateIfNeeded(final Long connectionId) {
        
        Set<Long> wideTableIds = connectionIdToWideTableIds.get(connectionId);
        if (Collections.isEmpty(wideTableIds)) {
            return;
        }
        wideTableIds.forEach(wideTableId -> {
            WideTableDTO wideTableDTO = wideTableInformer.get(wideTableId);
            if (wideTableDTO == null) {
                return;
            }
            getWideTableActualStateAndUpdateActualStateIfNeeded(wideTableDTO);
        });
    }
    
    private void addWideTableEventToTheQueue(final WideTableDTO wideTable) {
        events.add(() -> handleWideTableEvent(wideTable));
    }
    
    private void handleWideTableEvent(final WideTableDTO wideTableDTO) {
        buildConnectionWideTableRelationsForController(wideTableDTO);
        buildRequisitionBatchWideTableRelationForController(wideTableDTO);
        
        Long requisitionBatchId = wideTableDTO.getRequisitionBatchId();
        if (wideTableDTO.getRequisitionState() == RequisitionState.APPROVING) {
            if (requisitionBatchId != null) {
                DataSystemResourcePermissionRequisitionBatchDTO requisitionBatchDTO = requisitionBatchInformer.get(requisitionBatchId);
                handleWideTableRequisitionState(wideTableDTO, requisitionBatchDTO);
                return;
            }
            wideTableService.createWideTableRequisitionBatch(wideTableDTO);
        }
        
        if (wideTableDTO.getRequisitionState() == RequisitionState.APPROVED) {
            takeActionsAndUpdateActualStateIfNeeded(wideTableDTO);
        }
    }
    
    private void handleWideTableRequisitionState(final WideTableDTO wideTableDTO, final DataSystemResourcePermissionRequisitionBatchDTO requisitionBatchDTO) {
        if (requisitionBatchDTO == null) {
            return;
        }
        updateWideTableRequisitionStateIfNeeded(requisitionBatchDTO.getState(), wideTableDTO.getId(), wideTableDTO.getRequisitionState());
    }
    
    private void buildRequisitionBatchWideTableRelationForController(final WideTableDTO wideTableDTO) {
        if (wideTableDTO.getRequisitionBatchId() != null) {
            requisitionBatchIdToWideTableIds.put(wideTableDTO.getRequisitionBatchId(), wideTableDTO.getId());
        }
    }
    
    private void takeActionsAndUpdateActualStateIfNeeded(final WideTableDTO wideTableDTO) {
        if (Objects.equals(wideTableDTO.getDesiredState(), WideTableState.READY)) {
            if (Objects.requireNonNull(wideTableDTO.getActualState()) == WideTableState.DISABLED) {
                wideTableService.startInnerConnectionAndUpdateActualStateToLoading(wideTableDTO.getId());
            } else {
                getWideTableActualStateAndUpdateActualStateIfNeeded(wideTableDTO);
            }
        }
        if (Objects.equals(wideTableDTO.getDesiredState(), WideTableState.DISABLED)) {
            switch (wideTableDTO.getActualState()) {
                case READY:
                case LOADING:
                case ERROR:
                    wideTableService.updateWideTableActualState(wideTableDTO.getId(), WideTableState.DISABLED);
                    break;
                default:
                    break;
            }
        }
    }
    
    private void getWideTableActualStateAndUpdateActualStateIfNeeded(final WideTableDTO wideTableDTO) {
        WideTableState newActualState = getWideTableState(wideTableDTO);
        if (newActualState != wideTableDTO.getActualState()) {
            wideTableService.updateWideTableActualState(wideTableDTO.getId(), newActualState);
        }
    }
    
    private WideTableState getWideTableState(final WideTableDTO wideTableDTO) {
        AtomicBoolean isReady = new AtomicBoolean(true);
        AtomicBoolean isError = new AtomicBoolean(false);
        wideTableDTO.getRelatedConnectionIds().stream().map(connectionInformer::get).forEach(connectionDTO -> {
            ConnectionState connectionActualState = connectionDTO == null ? null : connectionDTO.getActualState();
            if (connectionActualState == null) {
                isReady.set(false);
                return;
            }
            switch (connectionActualState) {
                case FAILED:
                    isReady.set(false);
                    isError.set(true);
                    break;
                case RUNNING:
                    break;
                default:
                    isReady.set(false);
            }
        });
        return isReady.get() ? WideTableState.READY
                : isError.get() ? WideTableState.ERROR
                : wideTableDTO.getActualState();
    }
    
    private void buildConnectionWideTableRelationsForController(final WideTableDTO wideTableDTO) {
        if (wideTableDTO.getRelatedConnectionIds() != null) {
            wideTableDTO.getRelatedConnectionIds().forEach(connectionId ->
                    connectionIdToWideTableIds.computeIfAbsent(connectionId, k -> new HashSet<>()).add(wideTableDTO.getId())
            );
        }
    }
    
    @Override
    void doStart() {
        requisitionBatchInformer.start();
        wideTableInformer.start();
        connectionInformer.start();
        
        new Thread(this::execute).start();
    }
    
    private void execute() {
        while (enableRunning) {
            try {
                events.take().run();
                // CHECKSTYLE:OFF
            } catch (Exception e) {
                // CHECKSTYLE:ON
                log.error("Wide table runner exception: ", e);
            }
        }
    }
    
    @Override
    void doStop() {
        requisitionBatchInformer.stop();
        wideTableInformer.stop();
        connectionInformer.stop();
        
        enableRunning = false;
    }
}
