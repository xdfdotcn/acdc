package cn.xdf.acdc.devops.service.process.datasystem.hive.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.Dataset4ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.HdfsDTO;
import cn.xdf.acdc.devops.core.domain.dto.HiveDTO;
import cn.xdf.acdc.devops.core.domain.dto.HiveDatabaseDTO;
import cn.xdf.acdc.devops.core.domain.dto.HiveTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.DatasetFrom;
import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.HiveQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.HiveRepository;
import cn.xdf.acdc.devops.repository.HiveTableRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.entity.HiveService;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDatasetProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HdfsProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDatabaseProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveTableProcessService;
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
public class HiveDatasetProcessServiceImpl extends AbstractDatasetProcessService {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private HiveProcessService hiveProcessService;

    @Autowired
    private HdfsProcessService hdfsProcessService;

    @Autowired
    private HiveDatabaseProcessService hiveDatabaseProcessService;

    @Autowired
    private HiveTableProcessService hiveTableProcessService;

    @Autowired
    private HiveTableRepository hiveTableRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private HiveRepository hiveRepository;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.HIVE;
    }

    @Override
    public List<DatasetClusterDTO> queryDatasetCluster(final DatasetClusterDTO cluster) {
        ProjectDO projectHive = projectService.findById(cluster.getProjectId())
                .orElseThrow(() -> new NotFoundException(String.format("projectId: %s", cluster.getProjectId())));

        List<Long> hiveIds = projectHive.getHives().stream().map(HiveDO::getId).collect(Collectors.toList());
        HiveQuery hiveQuery = HiveQuery.builder()
                .hiveIds(hiveIds)
                .name(cluster.getName())
                .build();

        List<HiveDO> hiveList = CollectionUtils.isEmpty(hiveIds) ? Collections.EMPTY_LIST : hiveRepository.queryAll(hiveQuery);
        return hiveList.stream().map(DatasetClusterDTO::new).collect(Collectors.toList());
    }

    @Override
    public void verifyDataset(final List<DataSetDTO> datasets, final DatasetFrom from) {
        BizAssert.innerError(DatasetFrom.SINK == from, "Hive dataset not supported as source ");
        verifyProject(datasets);
        Set<Long> ids = datasets.stream().map(DataSetDTO::getDataSetId).collect(Collectors.toSet());
        getHiveTables(ids);
    }

    @Override
    public Dataset4ConnectionDTO getSourceDataset4Connection(final ConnectionDTO connectionDTO) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Dataset4ConnectionDTO getSinkDataset4Connection(final ConnectionDTO connectionDTO) {
        Long sinkProjectId = connectionDTO.getSinkProjectId();
        Long sinkDatasetId = connectionDTO.getSinkDataSetId();

        HiveTableDTO hiveTableDTO = hiveTableProcessService.getHiveTable(sinkDatasetId);
        HiveDatabaseDTO hiveDatabaseDTO = hiveDatabaseProcessService.getHiveDatabase(hiveTableDTO.getDatabaseId());
        HiveDTO hiveDTO = hiveProcessService.getHive(hiveDatabaseDTO.getClusterId());
        HdfsDTO hdfsDTO = hdfsProcessService.getHdfs(hiveDTO.getHdfsId());
        ProjectDTO projectDTO = getProject(sinkProjectId);

        return Dataset4ConnectionDTO.builder()
                .dataSystemType(DataSystemType.HIVE)

                .projectId(projectDTO.getId())
                .projectName(projectDTO.getName())

                .clusterId(hiveDTO.getId())
                .clusterName(hiveDTO.getName())

                .databaseId(hiveDatabaseDTO.getId())
                .databaseName(hiveDatabaseDTO.getName())

                .dataSetId(hiveTableDTO.getId())
                .datasetName(hiveTableDTO.getName())

                .instanceId(hdfsDTO.getId())
                .build();
    }

    @Override
    public Page<DatasetInstanceDTO> queryDatasetInstance(final DatasetInstanceDTO instance) {
        Pageable pageable = PagedQuery.ofPage(instance.getCurrent(), instance.getPageSize());

        HiveDO hive = hiveService.findById(instance.getClusterId())
                .orElseThrow(() -> new NotFoundException(String.format("clusterId: %s", instance.getClusterId())));

        HdfsDO hdfs = hive.getHdfs();

        List<DatasetInstanceDTO> instances = Lists.newArrayListWithCapacity(1);
        instances.add(new DatasetInstanceDTO(hdfs));

        return new PageImpl<>(instances, pageable, instances.size());
    }
}
