package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class ConnectionInformer extends AbstractInformer<ConnectionDetailDTO> {

    private final ConnectionProcessService connectionProcessService;

    public ConnectionInformer(final TaskScheduler taskScheduler, final ConnectionProcessService connectionProcessService) {
        super(taskScheduler);
        this.connectionProcessService = connectionProcessService;
    }

    @Override
    List<ConnectionDetailDTO> query() {
        ConnectionQuery query = ConnectionQuery.builder().beginUpdateTime(super.getLastUpdateTime())
                .requisitionState(RequisitionState.APPROVED).build();
        return connectionProcessService.query(query);
    }

    @Override
    Long getKey(final ConnectionDetailDTO element) {
        return element.getId();
    }

    @Override
    boolean equals(final ConnectionDetailDTO e1, final ConnectionDetailDTO e2) {
        return Objects.equals(e1.getDesiredState(), e2.getDesiredState())
                && Objects.equals(e1.getActualState(), e2.getActualState())
                && Objects.equals(e1.getSinkConnectorId(), e2.getSinkConnectorId())
                && Objects.equals(e1.getSourceConnectorId(), e2.getSourceConnectorId());
    }

    @Override
    boolean isDeleted(final ConnectionDetailDTO older, final ConnectionDetailDTO newer) {
        return !older.getDeleted() && newer.getDeleted();
    }

    @Override
    Instant getUpdateTime(final ConnectionDetailDTO connectionDetailDTO) {
        return connectionDetailDTO.getUpdateTime();
    }

}
