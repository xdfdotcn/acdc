package cn.xdf.acdc.devops.controller;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.informer.AbstractFixedRateRunnableInformer;
import cn.xdf.acdc.devops.informer.DataSystemResourcePermissionRequisitionBatchInformer;
import cn.xdf.acdc.devops.informer.DataSystemResourcePermissionRequisitionInformer;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalContext;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalEvent;
import cn.xdf.acdc.devops.service.process.connection.approval.event.ApprovalOperation;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

@Component
@Slf4j
public class DataSystemResourcePermissionRequisitionController extends Controller {
    
    private final DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService;
    
    private final AbstractFixedRateRunnableInformer<DataSystemResourcePermissionRequisitionBatchDTO> requisitionBatchInformer;
    
    private final AbstractFixedRateRunnableInformer<DataSystemResourcePermissionRequisitionDTO> requisitionInformer;
    
    private final ApprovalStateMachine approvalStateMachine;
    
    private volatile boolean enableRunning = true;
    
    private final LinkedBlockingQueue<Runnable> events = new LinkedBlockingQueue<>();
    
    private final Map<Long, Set<Long>> requisitionIdToBatchIds = new HashMap<>();
    
    public DataSystemResourcePermissionRequisitionController(final DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService,
                                                             final DataSystemResourcePermissionRequisitionService dataSystemResourcePermissionRequisitionService,
                                                             final TaskScheduler taskScheduler, final ApprovalStateMachine approvalStateMachine) {
        this.dataSystemResourcePermissionRequisitionBatchService = dataSystemResourcePermissionRequisitionBatchService;
        this.requisitionBatchInformer = new DataSystemResourcePermissionRequisitionBatchInformer(taskScheduler, dataSystemResourcePermissionRequisitionBatchService)
                .whenAdd(this::addRequisitionBatchEventToTheQueue)
                .whenUpdate(this::addRequisitionBatchEventToTheQueue);
        this.requisitionInformer = new DataSystemResourcePermissionRequisitionInformer(taskScheduler, dataSystemResourcePermissionRequisitionService)
                .whenAdd(this::addRequisitionEventToTheQueue)
                .whenUpdate(this::addRequisitionEventToTheQueue);
        this.approvalStateMachine = approvalStateMachine;
    }
    
    private void addRequisitionEventToTheQueue(final DataSystemResourcePermissionRequisitionDTO dataSystemResourcePermissionRequisitionDTO) {
        events.add(() -> handleRequisitionEvent(dataSystemResourcePermissionRequisitionDTO));
    }
    
    private void handleRequisitionEvent(final DataSystemResourcePermissionRequisitionDTO requisitionDTO) {
        switch (requisitionDTO.getState()) {
            case APPROVED:
            case DBA_REFUSED:
            case SOURCE_OWNER_REFUSED:
                Set<Long> batchIds = requisitionIdToBatchIds.get(requisitionDTO.getId());
                if (batchIds == null) {
                    return;
                }
                batchIds.forEach(batchId -> {
                    DataSystemResourcePermissionRequisitionBatchDTO batchDTO = requisitionBatchInformer.get(batchId);
                    if (batchDTO != null) {
                        updateBatchStateIfNeeded(batchDTO);
                    }
                });
                break;
            case APPROVING:
                ApprovalContext approvalContext = new ApprovalContext()
                        .setId(requisitionDTO.getId())
                        .setDescription(requisitionDTO.getDescription())
                        .setOperatorId(requisitionDTO.getUserDomainAccount());
                ApprovalEvent event = approvalStateMachine.getApprovalEventGenerator().generateApprovalEvent(requisitionDTO.getId(), ApprovalOperation.PASS);
                approvalStateMachine.fire(event, approvalContext);
                break;
            default:
                break;
        }
    }
    
    private void addRequisitionBatchEventToTheQueue(final DataSystemResourcePermissionRequisitionBatchDTO dataSystemResourcePermissionRequisitionBatchDTO) {
        events.add(() -> handleRequisitionBatchEvent(dataSystemResourcePermissionRequisitionBatchDTO));
    }
    
    private void handleRequisitionBatchEvent(final DataSystemResourcePermissionRequisitionBatchDTO batchDTO) {
        buildRelationForController(batchDTO);
        
        switch (batchDTO.getState()) {
            case PENDING:
                dataSystemResourcePermissionRequisitionBatchService.updateState(batchDTO.getId(), ApprovalBatchState.APPROVING);
                break;
            case APPROVING:
                updateBatchStateIfNeeded(batchDTO);
                break;
            default:
                break;
        }
    }
    
    private void updateBatchStateIfNeeded(final DataSystemResourcePermissionRequisitionBatchDTO batchDTO) {
        ApprovalBatchState currentState = getBatchDTOCurrentState(batchDTO);
        if (currentState != batchDTO.getState()) {
            dataSystemResourcePermissionRequisitionBatchService.updateState(batchDTO.getId(), currentState);
        }
    }
    
    private ApprovalBatchState getBatchDTOCurrentState(final DataSystemResourcePermissionRequisitionBatchDTO batchDTO) {
        boolean isRefused = false;
        boolean isApproving = false;
        
        for (Long requisitionId : batchDTO.getDataSystemResourcePermissionRequisitionIds()) {
            DataSystemResourcePermissionRequisitionDTO requisition = requisitionInformer.get(requisitionId);
            if (requisition == null) {
                return batchDTO.getState();
            }
            if (ApprovalState.REFUSED_STATES.contains(requisition.getState())) {
                isRefused = true;
            }
            if (ApprovalState.APPROVED != requisition.getState()) {
                isApproving = true;
            }
        }
        return isRefused ? ApprovalBatchState.REFUSED
                : isApproving ? ApprovalBatchState.APPROVING
                : ApprovalBatchState.APPROVED;
    }
    
    private void buildRelationForController(final DataSystemResourcePermissionRequisitionBatchDTO batchDTO) {
        batchDTO.getDataSystemResourcePermissionRequisitionIds().forEach(requisitionId ->
                requisitionIdToBatchIds.computeIfAbsent(requisitionId, k -> new HashSet<>()).add(batchDTO.getId()));
    }
    
    @Override
    void doStart() {
        requisitionBatchInformer.start();
        requisitionInformer.start();
        
        new Thread(this::execute).start();
    }
    
    private void execute() {
        while (enableRunning) {
            try {
                events.take().run();
                // CHECKSTYLE:OFF
            } catch (Exception e) {
                // CHECKSTYLE:ON
                log.error("Requisition runner exception: ", e);
            }
        }
    }
    
    @Override
    void doStop() {
        requisitionBatchInformer.stop();
        requisitionInformer.stop();
        
        enableRunning = false;
    }
}
