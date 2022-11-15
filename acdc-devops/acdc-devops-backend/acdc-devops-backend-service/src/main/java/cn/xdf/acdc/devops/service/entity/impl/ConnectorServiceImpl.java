package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.CreationResult;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.repository.ConnectorRepository;
import cn.xdf.acdc.devops.service.entity.ConnectClusterService;
import cn.xdf.acdc.devops.service.entity.ConnectorClassService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.entity.SourceRdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class ConnectorServiceImpl implements ConnectorService {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private SourceRdbTableService sourceRdbTableService;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Autowired
    private ConnectClusterService connectClusterService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private RdbTableService rdbTableService;

    @Override
    public CreationResult<ConnectorDO> saveSourceIfAbsent(
            final Long dataBaseId,
            final String connectorName,
            final DataSystemType dataSystemType,
            final ConnectorType connectorType
    ) {
        // TODO 目前没有 ip+port+database+table=connector的关系,因为source是database层级的，同一个database对一个一个connector
        RdbTableDO query = RdbTableDO.builder()
                .rdbDatabase(RdbDatabaseDO.builder()
                        .id(dataBaseId)
                        .build())
                .build();
        List<Long> rdbTableIdList = rdbTableService.queryAll(query)
                .stream().map(RdbTableDO::getId).collect(Collectors.toList());

        Optional<SourceRdbTableDO> anySourceRdbTable = sourceRdbTableService.queryByRdbTableIdList(rdbTableIdList).stream()
                .findFirst();

        // 如果此库已经存在connector实例，则不ConnectorServiceImpl需要再继续创建
        return anySourceRdbTable.isPresent()
                ? CreationResult.<ConnectorDO>builder().result(anySourceRdbTable.get().getConnector()).isPresent(true).build()
                : CreationResult.<ConnectorDO>builder().result(save(connectorName, dataSystemType, connectorType)).isPresent(false).build();
    }

    @Override
    public ConnectorDO save(final ConnectorDO connector) {
        return connectorRepository.save(connector);
    }

    @Override
    public ConnectorDO save(
            final String connectorName,
            final DataSystemType dataSystemType,
            final ConnectorType connectorType
    ) {
        String className = connectorType == ConnectorType.SOURCE
                ? dataSystemType.getSourceConnectorClass()
                : dataSystemType.getSinkConnectorClass();

        // 必须存在connector class
        ConnectorClassDO connectorClass = connectorClassService.findByClass(className, dataSystemType)
                .orElseThrow(() -> new NotFoundException(String.format("className: %s", className)));

        // 必须存在 connect cluster
        ConnectClusterDO connectCluster = connectClusterService.findByConnectorClassId(connectorClass.getId())
                .orElseThrow(() -> new NotFoundException(String.format("connectorClassId: %s", connectorClass.getId())));

        // connector 集群,默认只有一个kafka集群
        KafkaClusterDO kafkaCluster = kafkaClusterService.findInnerKafkaCluster()
                .orElseThrow(() -> new NotFoundException(String.format("type: %s", KafkaClusterType.INNER)));

        ConnectorDO newConnector = ConnectorDO.builder()
                .name(connectorName)
                .connectorClass(connectorClass)
                .connectCluster(connectCluster)
                .kafkaCluster(kafkaCluster)
                .actualState(ConnectorState.PENDING)
                .desiredState(ConnectorState.RUNNING)
                .creationTime(new Date().toInstant())
                .updateTime(new Date().toInstant())
                .build();

        return connectorService.save(newConnector);
    }

    @Override
    public List<ConnectorDO> saveAll(final List<ConnectorDO> connectorList) {
        return connectorRepository.saveAll(connectorList);
    }

    @Override
    public Page<ConnectorDO> pageQuery(final ConnectorQuery connectorQuery, final Pageable pageable) {
        return connectorRepository.findAll(ConnectorService.specificationOf(connectorQuery), pageable);
    }

    @Override
    public List<ConnectorDO> query(final ConnectorQuery query) {
        return connectorRepository.findAll(ConnectorService.specificationOf(query));
    }

    @Override
    public Optional<ConnectorDO> findById(final Long id) {
        return connectorRepository.findById(id);
    }
}
