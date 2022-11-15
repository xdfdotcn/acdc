package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.service.process.connector.ConnectorQueryProcessService;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class ConnectorInformer extends AbstractInformer<ConnectorDTO> {

    private final ConnectorQueryProcessService connectorQueryProcessService;

    public ConnectorInformer(final TaskScheduler scheduler, final ConnectorQueryProcessService connectorQueryProcessService) {
        super(scheduler);
        this.connectorQueryProcessService = connectorQueryProcessService;
    }

    @Override
    List<ConnectorDTO> query() {
        ConnectorQuery query = ConnectorQuery.builder().beginUpdateTime(super.getLastUpdateTime()).build();
        return connectorQueryProcessService.query(query);
    }

    @Override
    Long getKey(final ConnectorDTO element) {
        return element.getId();
    }

    @Override
    boolean equals(final ConnectorDTO e1, final ConnectorDTO e2) {
        return Objects.equals(e1.getActualState(), e2.getActualState());
    }

    @Override
    boolean isDeleted(final ConnectorDTO older, final ConnectorDTO newer) {
        return false;
    }

    @Override
    Instant getUpdateTime(final ConnectorDTO connectorDTO) {
        return connectorDTO.getUpdateTime();
    }

}
