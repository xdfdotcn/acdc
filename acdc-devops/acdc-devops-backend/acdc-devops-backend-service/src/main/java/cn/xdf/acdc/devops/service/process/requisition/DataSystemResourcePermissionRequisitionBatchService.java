package cn.xdf.acdc.devops.service.process.requisition;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionBatchQuery;

import java.util.List;
import java.util.Map;

public interface DataSystemResourcePermissionRequisitionBatchService {
    
    /**
     * Query resource permission requisition batch list with condition.
     *
     * @param query query object
     * @return query result
     */
    List<DataSystemResourcePermissionRequisitionBatchDTO> query(DataSystemResourcePermissionRequisitionBatchQuery query);
    
    /**
     * Update state.
     *
     * @param batchId batch id
     * @param state state
     */
    void updateState(Long batchId, ApprovalBatchState state);
    
    /**
     * Create permission requisition batch by data system resource and project.
     *
     * @param userId sink user who use the wide table
     * @param description the reason why apply the wide table data
     * @param sinkProjectId the project wide table belong to
     * @param dataSystemResourceIdProjectIdMap data system resource id and project id
     * @return batch id
     */
    Long create(Long userId, String description, Long sinkProjectId, Map<Long, Long> dataSystemResourceIdProjectIdMap);
}
