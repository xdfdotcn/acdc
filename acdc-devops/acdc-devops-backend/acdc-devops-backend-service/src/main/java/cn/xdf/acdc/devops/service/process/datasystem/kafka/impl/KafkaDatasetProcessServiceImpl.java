package cn.xdf.acdc.devops.service.process.datasystem.kafka.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.Dataset4ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.DatasetFrom;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDatasetProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaTopicProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.SpecificConfigurationProcessService;
import cn.xdf.acdc.devops.service.util.BizAssert;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class KafkaDatasetProcessServiceImpl extends AbstractDatasetProcessService {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    private SpecificConfigurationProcessService specificConfigurationProcessService;

    @Autowired
    private KafkaTopicProcessService kafkaTopicProcessService;

    @Autowired
    private KafkaClusterProcessServiceImpl kafkaClusterProcessServiceImpl;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.KAFKA;
    }

    @Override
    public List<DatasetClusterDTO> queryDatasetCluster(final DatasetClusterDTO cluster) {
        ProjectDO project = projectService.findById(cluster.getProjectId())
                .orElseThrow(() -> new NotFoundException(String.format("projectId: %s", cluster.getProjectId())));

        List<Long> kafkaClusterIds = project.getKafkaClusters().stream().map(KafkaClusterDO::getId).collect(Collectors.toList());
        KafkaClusterQuery query = KafkaClusterQuery.builder()
                .kafkaClusterIds(kafkaClusterIds)

                // 只查询用户的集群,屏蔽CDC内部使用的kafka集群
                .clusterType(KafkaClusterType.USER)
                .bootstrapServers(cluster.getName())
                .build();

        List<KafkaClusterDO> kafkaClusterList = CollectionUtils.isEmpty(kafkaClusterIds) ? Collections.EMPTY_LIST : kafkaClusterRepository.queryAll(query);
        return kafkaClusterList.stream().map(DatasetClusterDTO::new).collect(Collectors.toList());
    }

    @Override
    public void verifyDataset(final List<DataSetDTO> datasets, final DatasetFrom from) {
        BizAssert.innerError(DatasetFrom.SINK == from, "Kafka dataset not supported as source ");
        verifyProject(datasets);
        Set<Long> ids = datasets.stream().map(DataSetDTO::getDataSetId).collect(Collectors.toSet());
        getKafkaTopics(ids);

        datasets.forEach(it -> specificConfigurationProcessService.kafkaSpecificConfDeserialize(it.getSpecificConfiguration()));
    }

    @Override
    public Dataset4ConnectionDTO getSourceDataset4Connection(final ConnectionDTO connectionDTO) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Dataset4ConnectionDTO getSinkDataset4Connection(final ConnectionDTO connectionDTO) {
        Long sinkProjectId = connectionDTO.getSinkProjectId();
        Long sinkDatasetId = connectionDTO.getSinkDataSetId();

        KafkaTopicDTO kafkaTopicDTO = kafkaTopicProcessService.getKafkaTopic(sinkDatasetId);
        KafkaClusterDTO kafkaClusterDTO = kafkaClusterProcessServiceImpl.getKafkaCluster(kafkaTopicDTO.getKafkaClusterId());
        ProjectDTO project = getProject(sinkProjectId);

        return Dataset4ConnectionDTO.builder()
                .dataSystemType(DataSystemType.KAFKA)

                .projectId(project.getId())
                .projectName(project.getName())

                .clusterId(kafkaClusterDTO.getId())
                .clusterName(kafkaClusterDTO.getName())

                .dataSetId(kafkaTopicDTO.getId())
                .datasetName(kafkaTopicDTO.getName())

                .instanceId(kafkaClusterDTO.getId())
                .build();
    }

    @Override
    public Page<DatasetInstanceDTO> queryDatasetInstance(final DatasetInstanceDTO instance) {
        Pageable pageable = PagedQuery.ofPage(instance.getCurrent(), instance.getPageSize());

        KafkaClusterDO kafkaCluster = kafkaClusterService.findById(instance.getClusterId())
                .orElseThrow(() -> new NotFoundException(String.format("clusterId: %s", instance.getClusterId())));

        List<DatasetInstanceDTO> instances = Lists.newArrayListWithCapacity(1);
        instances.add(new DatasetInstanceDTO(kafkaCluster));

        return new PageImpl<>(instances, pageable, instances.size());
    }
}
