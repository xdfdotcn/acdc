package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import org.springframework.scheduling.TaskScheduler;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class ConnectionInformer extends AbstractFixedRateRunnableInformer<ConnectionDTO> {
    
    private final ConnectionService connectionService;
    
    public ConnectionInformer(final TaskScheduler taskScheduler, final ConnectionService connectionService) {
        super(taskScheduler);
        this.connectionService = connectionService;
    }
    
    @Override
    List<ConnectionDTO> query() {
        ConnectionQuery query = new ConnectionQuery()
                .setBeginUpdateTime(super.getLastUpdateTime())
                .setRequisitionState(RequisitionState.APPROVED);
        return connectionService.query(query);
    }
    
    @Override
    Long getKey(final ConnectionDTO element) {
        return element.getId();
    }
    
    @Override
    boolean equals(final ConnectionDTO e1, final ConnectionDTO e2) {
        return Objects.equals(e1.getDesiredState(), e2.getDesiredState())
                && Objects.equals(e1.getActualState(), e2.getActualState())
                && Objects.equals(e1.getSinkConnectorId(), e2.getSinkConnectorId())
                && Objects.equals(e1.getSourceConnectorId(), e2.getSourceConnectorId());
    }
    
    @Override
    boolean isDeleted(final ConnectionDTO older, final ConnectionDTO newer) {
        return !older.isDeleted() && newer.isDeleted();
    }
    
    @Override
    Date getUpdateTime(final ConnectionDTO connectionDetailDTO) {
        return connectionDetailDTO.getUpdateTime();
    }
    
}
