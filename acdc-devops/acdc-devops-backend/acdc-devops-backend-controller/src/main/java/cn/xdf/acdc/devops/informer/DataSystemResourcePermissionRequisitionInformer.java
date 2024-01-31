package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourcePermissionRequisitionQuery;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionService;
import org.springframework.scheduling.TaskScheduler;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class DataSystemResourcePermissionRequisitionInformer extends AbstractFixedRateRunnableInformer<DataSystemResourcePermissionRequisitionDTO> {
    
    private final DataSystemResourcePermissionRequisitionService dataSystemResourcePermissionRequisitionService;
    
    public DataSystemResourcePermissionRequisitionInformer(final TaskScheduler scheduler,
                                                           final DataSystemResourcePermissionRequisitionService dataSystemResourcePermissionRequisitionService) {
        super(scheduler);
        this.dataSystemResourcePermissionRequisitionService = dataSystemResourcePermissionRequisitionService;
    }
    
    @Override
    List<DataSystemResourcePermissionRequisitionDTO> query() {
        DataSystemResourcePermissionRequisitionQuery query = new DataSystemResourcePermissionRequisitionQuery()
                .setBeginUpdateTime(super.getLastUpdateTime());
        return dataSystemResourcePermissionRequisitionService.query(query);
    }
    
    @Override
    Long getKey(final DataSystemResourcePermissionRequisitionDTO element) {
        return element.getId();
    }
    
    @Override
    boolean equals(final DataSystemResourcePermissionRequisitionDTO e1, final DataSystemResourcePermissionRequisitionDTO e2) {
        return Objects.equals(e1, e2);
    }
    
    @Override
    boolean isDeleted(final DataSystemResourcePermissionRequisitionDTO older, final DataSystemResourcePermissionRequisitionDTO newer) {
        return !older.isDeleted() && newer.isDeleted();
    }
    
    @Override
    Date getUpdateTime(final DataSystemResourcePermissionRequisitionDTO dataSystemResourcePermissionRequisitionDTO) {
        return dataSystemResourcePermissionRequisitionDTO.getUpdateTime();
    }
}
