package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connector.ConnectorQueryProcessService;
import com.google.common.base.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Transactional
public class ConnectorQueryProcessServiceImpl implements ConnectorQueryProcessService {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private SinkConnectorProcessServiceManager sinkConnectorProcessServiceManager;

    @Autowired
    private SourceConnectorProcessServiceManager sourceConnectorProcessServiceManager;

    @Override
    public List<ConnectorDTO> query(final ConnectorQuery query) {
        return connectorService.query(query).stream().map(ConnectorDTO::new).collect(Collectors.toList());
    }

    @Override
    public Page<ConnectorDTO> pageQuery(final ConnectorQuery connectorQuery) {
        Pageable pageable = PagedQuery.ofPage(connectorQuery.getCurrent(), connectorQuery.getPageSize());
        return connectorService.pageQuery(connectorQuery, pageable).map(ConnectorDTO::new);
    }

    @Override
    public Page<SinkConnectorInfoDTO> querySinkForSource(
            final Long sourceConnectorId,
            final Pageable pageable,
            final DataSystemType dataSystemType) {
        return sinkConnectorProcessServiceManager.getJService(dataSystemType)
                .querySinkForSource(sourceConnectorId, pageable);
    }

    @Override
    public SinkConnectorInfoDTO getSinkInfo(final Long connectorId) {
        ConnectorDO connector = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        ConnectorClassDO connectorClass = connector.getConnectorClass();
        Preconditions.checkArgument(ConnectorType.SINK.equals(connectorClass.getConnectorType()));

        DataSystemType dataSystemType = connector.getConnectorClass().getDataSystemType();

        return sinkConnectorProcessServiceManager.getJService(dataSystemType).getSinkDetail(connectorId);
    }

    @Override
    public SourceConnectorInfoDTO getSourceInfo(final Long connectorId) {
        ConnectorDO connector = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        ConnectorClassDO connectorClass = connector.getConnectorClass();
        Preconditions.checkArgument(ConnectorType.SOURCE.equals(connectorClass.getConnectorType()));

        DataSystemType dataSystemType = connectorClass.getDataSystemType();

        return sourceConnectorProcessServiceManager.getJService(dataSystemType)
                .getSourceDetail(connectorId);
    }
}
