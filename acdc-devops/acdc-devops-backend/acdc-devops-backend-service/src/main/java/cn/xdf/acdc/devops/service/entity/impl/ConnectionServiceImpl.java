package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionInfoQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.service.entity.ConnectionService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.SystemBizException;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// CHECKSTYLE:OFF

@Service
public class ConnectionServiceImpl implements ConnectionService {

    @Autowired
    private ConnectionRepository connectionRepository;

    @Autowired
    private UserProcessService userProcessService;

    @Override
    public List<ConnectionDO> query(final ConnectionQuery query) {
        return connectionRepository.findAll(ConnectionService.specificationOf(query));
    }

    @Override
    public Page<ConnectionDO> pagingQuery(final ConnectionQuery query, final Pageable pageable) {
        return connectionRepository.findAll(ConnectionService.specificationOf(query), pageable);
    }

    @Override
    public Page<Map<String, Object>> detailPagingQuery(
            final DataSystemType sinkDataSystemType,
            final ConnectionInfoQuery query,
            final Pageable pageable,
            final String domainAccount
    ) {

        UserDTO currentUser = userProcessService.getUserByDomainAccount(domainAccount);
        boolean isAdmin = userProcessService.isAdmin(domainAccount);
        switch (sinkDataSystemType) {
            case MYSQL:
            case TIDB:
                return connectionRepository.findJdbcConnection(
                        pageable,
                        isAdmin,
                        currentUser.getId(),
                        Optional.ofNullable(query.getRequisitionState()).map(it -> it.ordinal()).orElse(null),
                        Optional.ofNullable(query.getActualState()).map(it -> it.ordinal()).orElse(null),
                        query.getSinkDatasetClusterName(),
                        query.getSinkDatasetDatabaseName(),
                        query.getSinkDatasetName(),
                        sinkDataSystemType.ordinal()
                );
            case HIVE:
                return connectionRepository.findHiveConnection(
                        pageable,
                        isAdmin,
                        currentUser.getId(),
                        Optional.ofNullable(query.getRequisitionState()).map(it -> it.ordinal()).orElse(null),
                        Optional.ofNullable(query.getActualState()).map(it -> it.ordinal()).orElse(null),
                        query.getSinkDatasetClusterName(),
                        query.getSinkDatasetDatabaseName(),
                        query.getSinkDatasetName()
                );

            case KAFKA:
                return connectionRepository.findKafkaConnection(
                        pageable,
                        isAdmin,
                        currentUser.getId(),
                        Optional.ofNullable(query.getRequisitionState()).map(it -> it.ordinal()).orElse(null),
                        Optional.ofNullable(query.getActualState()).map(it -> it.ordinal()).orElse(null),
                        query.getSinkDatasetClusterName(),
                        query.getSinkDatasetName()
                );

            default:
                throw new SystemBizException("UNKNOWN type: " + sinkDataSystemType);
        }
    }

    @Override
    public ConnectionDO save(final ConnectionDO connection) {
        return connectionRepository.save(connection);
    }

    @Override
    public List<ConnectionDO> saveAll(final List<ConnectionDO> connections) {
        return connectionRepository.saveAll(connections);
    }

    @Override
    public Optional<ConnectionDO> findById(final Long id) {
        return connectionRepository.findById(id);
    }

    @Override
    public List<ConnectionDO> findAllById(final Set<Long> ids) {
        return connectionRepository.findAllById(ids);
    }

    @Override
    public boolean existsEachInSinkDatasetIds(final List<Long> datasetIds) {
        return connectionRepository.countByDeletedFalseAndSinkDataSetIdIn(datasetIds) > 0;
    }

    @Override
    public List<ConnectionColumnConfigurationDO> findNewestColumnConfig(final Long connectionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateActualState(final Long connectionId, final ConnectionState state) {
        ConnectionDO connection = this.findById(connectionId)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connectionId)));
        connection.setActualState(state);
        connection.setUpdateTime(Instant.now());
        save(connection);
    }

    @Override
    public void updateDesiredState(final Long connectionId, final ConnectionState state) {
        ConnectionDO connection = this.findById(connectionId)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connectionId)));
        connection.setDesiredState(state);
        connection.setUpdateTime(Instant.now());
        save(connection);
    }
}
