package cn.xdf.acdc.devops.informer;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import org.springframework.scheduling.TaskScheduler;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class ConnectorInformer extends AbstractInformer<ConnectorDTO> {

    private final ConnectorService connectorService;

    public ConnectorInformer(final TaskScheduler scheduler, final ConnectorService connectorService) {
        super(scheduler);
        this.connectorService = connectorService;
    }

    @Override
    List<ConnectorDTO> query() {
        ConnectorQuery query = ConnectorQuery.builder().beginUpdateTime(super.getLastUpdateTime()).build();
        return connectorService.query(query);
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
    Date getUpdateTime(final ConnectorDTO connectorDTO) {
        return connectorDTO.getUpdateTime();
    }

}
