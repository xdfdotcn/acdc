package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDTO;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionBatchQuery;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import org.springframework.scheduling.TaskScheduler;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class DataSystemResourcePermissionRequisitionBatchInformer extends AbstractFixedRateRunnableInformer<DataSystemResourcePermissionRequisitionBatchDTO> {
    
    private final DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService;
    
    public DataSystemResourcePermissionRequisitionBatchInformer(final TaskScheduler scheduler,
                                                                final DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService) {
        super(scheduler);
        this.dataSystemResourcePermissionRequisitionBatchService = dataSystemResourcePermissionRequisitionBatchService;
    }
    
    @Override
    List<DataSystemResourcePermissionRequisitionBatchDTO> query() {
        DataSystemResourcePermissionRequisitionBatchQuery query = new DataSystemResourcePermissionRequisitionBatchQuery()
                .setBeginUpdateTime(super.getLastUpdateTime());
        return dataSystemResourcePermissionRequisitionBatchService.query(query);
    }
    
    @Override
    Long getKey(final DataSystemResourcePermissionRequisitionBatchDTO element) {
        return element.getId();
    }
    
    @Override
    boolean equals(final DataSystemResourcePermissionRequisitionBatchDTO e1, final DataSystemResourcePermissionRequisitionBatchDTO e2) {
        return Objects.equals(e1, e2);
    }
    
    @Override
    boolean isDeleted(final DataSystemResourcePermissionRequisitionBatchDTO older, final DataSystemResourcePermissionRequisitionBatchDTO newer) {
        return !older.isDeleted() && newer.isDeleted();
    }
    
    @Override
    Date getUpdateTime(final DataSystemResourcePermissionRequisitionBatchDTO dataSystemResourcePermissionRequisitionBatchDTO) {
        return dataSystemResourcePermissionRequisitionBatchDTO.getUpdateTime();
    }
}
