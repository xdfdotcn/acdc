package cn.xdf.acdc.devops.service.process.widetable;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import cn.xdf.acdc.devops.core.domain.query.WideTableQuery;
import org.springframework.data.domain.Page;

import java.util.List;

public interface WideTableService {
    
    /**
     * Create wide table related inner connection if needed.
     *
     * @param id wide table id
     */
    void createInnerConnectionIfNeeded(Long id);
    
    /**
     * Update wide table's actual state.
     *
     * @param id wide table id
     * @param newActualState new wide table actual state
     */
    void updateActualState(Long id, WideTableState newActualState);
    
    /**
     * Update wide table's requisition state.
     *
     * @param id wide table id
     * @param requisitionState new wide table's requisition state
     */
    void updateRequisitionState(Long id, RequisitionState requisitionState);
    
    /**
     * Query wide table list with condition.
     *
     * @param wideTableQuery query object
     * @return query result
     */
    List<WideTableDTO> query(WideTableQuery wideTableQuery);
    
    /**
     * Query wide table detail list with condition.
     *
     * @param wideTableQuery query object
     * @return query result
     */
    List<WideTableDetailDTO> queryDetail(WideTableQuery wideTableQuery);
    
    /**
     * Update wide table's requisition batch id.
     *
     * @param id wide table id
     * @param batchId related requisition batch id
     */
    void updateBatchId(Long id, Long batchId);
    
    /**
     * Validate wide table.
     *
     * @param wideTableDetail wide table detail DTO
     * @return wide table detail DTO
     */
    WideTableDetailDTO beforeCreation(WideTableDetailDTO wideTableDetail);
    
    /**
     * Create wide table.
     *
     * @param wideTableDetail wide table detail DTO
     * @return wide table detail DTO
     */
    WideTableDetailDTO create(WideTableDetailDTO wideTableDetail);
    
    /**
     * Get detail by id.
     *
     * @param wideTableId wide table id
     * @return wide table detail DTO
     */
    WideTableDetailDTO getDetailById(Long wideTableId);
    
    /**
     * Page query.
     *
     * @param query query
     * @return page
     */
    Page<WideTableDTO> pagedQuery(WideTableQuery query);
    
    /**
     * Disable the wide table.
     *
     * @param wideTableId wide table id
     */
    void disable(Long wideTableId);
    
    /**
     * Enable the wide table.
     *
     * @param wideTableId wide table id
     */
    void enable(Long wideTableId);
    
    /**
     * Get connection by wide table id.
     *
     * @param wideTableId wide table id
     * @return connections
     */
    List<ConnectionDTO> getConnectionsByWideTableId(Long wideTableId);
    
    /**
     * Get requisition by wide table id.
     *
     * @param wideTableId wide table id
     * @return requisition
     */
    DataSystemResourcePermissionRequisitionBatchDetailDTO getRequisitionByWideTableId(Long wideTableId);
    
    /**
     * Update requisition state to refused state.
     *
     * @param wideTableId wide table id
     */
    void updateRequisitionStateToRefused(Long wideTableId);
    
    /**
     * Create data system resource and update wide table.
     *
     * @param wideTableId wide table id
     */
    void createDataSystemResourceAndUpdateWideTable(Long wideTableId);
    
    /**
     * Create wide table requisition batch.
     *
     * @param wideTableDTO wide table dto
     */
    void createWideTableRequisitionBatch(WideTableDTO wideTableDTO);
    
    /**
     * Update wide table actual state.
     *
     * @param id wide table id
     * @param newActualState actual state
     */
    void updateWideTableActualState(Long id, WideTableState newActualState);
    
    /**
     * Start inner connection and update actual state to loading state.
     *
     * @param wideTableId wide table id
     */
    void startInnerConnectionAndUpdateActualStateToLoading(Long wideTableId);
}
