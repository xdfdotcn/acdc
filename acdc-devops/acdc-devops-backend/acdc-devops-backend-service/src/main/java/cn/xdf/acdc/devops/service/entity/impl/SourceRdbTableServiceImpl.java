package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.CreationResult;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.repository.SourceRdbTableRepository;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.SourceRdbTableService;
import cn.xdf.acdc.devops.service.error.SystemBizException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
public class SourceRdbTableServiceImpl implements SourceRdbTableService {

    @Autowired
    private SourceRdbTableRepository sourceRdbTableRepository;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Override
    public SourceRdbTableDO save(final SourceRdbTableDO sourceRdbTable) {
        return sourceRdbTableRepository.save(sourceRdbTable);
    }

    @Override
    public SourceRdbTableDO save(
            final Long rdbTableId,
            final Long connectorId,
            final String topic) {
        Preconditions.checkNotNull(rdbTableId, "RdbTableId is null");
        Preconditions.checkNotNull(rdbTableId, "ConnectorId is null");
        Preconditions.checkNotNull(rdbTableId, "Topic is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "Topic is empty");

        KafkaClusterDO kafkaCluster = kafkaClusterService.findInnerKafkaCluster()
                .orElseThrow(() -> new SystemBizException(String.format("Not found, type: %s", KafkaClusterType.INNER)));

        KafkaTopicDO kafkaTopic = kafkaTopicService.save(KafkaTopicDO.builder()
                .name(topic)
                .kafkaCluster(kafkaCluster)
                .build());

        return save(SourceRdbTableDO.builder()
                .connector(new ConnectorDO().setId(connectorId))
                .rdbTable(RdbTableDO.builder().id(rdbTableId).build())
                .kafkaTopic(kafkaTopic)
                .creationTime(new Date().toInstant())
                .updateTime(new Date().toInstant())
                .build());
    }

    @Override
    public CreationResult<SourceRdbTableDO> saveIfAbsent(
            final Long rdbTableId,
            final Long connectorId,
            final String topic) {

        Optional<SourceRdbTableDO> sourceRdbTableOpt = findByRdbTableId(rdbTableId);

        return sourceRdbTableOpt.isPresent()
                ? CreationResult.<SourceRdbTableDO>builder().result(sourceRdbTableOpt.get()).isPresent(true).build()
                : CreationResult.<SourceRdbTableDO>builder().result(save(rdbTableId, connectorId, topic)).isPresent(false).build();
    }

    @Override
    public List<SourceRdbTableDO> saveAll(final List<SourceRdbTableDO> sourceRdbTableList) {
        return sourceRdbTableRepository.saveAll(sourceRdbTableList);
    }

    @Override
    public List<SourceRdbTableDO> findAll() {
        return sourceRdbTableRepository.findAll();
    }

    @Override
    public Optional<SourceRdbTableDO> findById(final Long id) {
        return sourceRdbTableRepository.findById(id);
    }

    @Override
    public List<SourceRdbTableDO> findByConnectorId(final Long connectorId) {
        return sourceRdbTableRepository.findOneByConnectorId(connectorId);
    }

    @Override
    public Optional<SourceRdbTableDO> findByRdbTableId(final Long rdbTableId) {
        return sourceRdbTableRepository.findOneByRdbTableId(rdbTableId);
    }

    @Override
    public Optional<SourceRdbTableDO> findByKafkaTopicId(final Long kafkaTopicId) {
        return sourceRdbTableRepository.findOneByKafkaTopicId(kafkaTopicId);
    }

    @Override
    public List<SourceRdbTableDO> queryByRdbTableIdList(final List<Long> rdbTableIdList) {
        return sourceRdbTableRepository.findAll(SourceRdbTableService.specificationOf(rdbTableIdList));
    }
}
