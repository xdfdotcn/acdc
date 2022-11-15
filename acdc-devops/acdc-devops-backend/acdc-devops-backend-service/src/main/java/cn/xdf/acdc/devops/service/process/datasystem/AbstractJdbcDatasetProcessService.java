package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.Dataset4ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDatabaseDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.DatasetFrom;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.entity.ProjectService;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.entity.RdbService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbDatabaseProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbInstanceProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbTableProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractJdbcDatasetProcessService extends AbstractDatasetProcessService {

    @Autowired
    private RdbService rdbService;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private RdbTableProcessService rdbTableProcessService;

    @Autowired
    private RdbDatabaseProcessService rdbDatabaseProcessService;

    @Autowired
    private RdbProcessService rdbProcessService;

    @Autowired
    private RdbInstanceProcessService rdbInstanceProcessService;

    @Override
    public List<DatasetClusterDTO> queryDatasetCluster(final DatasetClusterDTO cluster) {
        ProjectDO projectRdb = projectService.findById(cluster.getProjectId())
                .orElseThrow(() -> new NotFoundException(String.format("projectId: %s", cluster.getProjectId())));

        List<Long> rdbIds = projectRdb.getRdbs().stream().map(RdbDO::getId).collect(Collectors.toList());
        RdbQuery rdbQuery = RdbQuery.builder()
                .rdbIds(rdbIds)
                .name(cluster.getName())
                .build();

        List<RdbDO> rdbList = CollectionUtils.isEmpty(rdbIds) ? Collections.EMPTY_LIST : rdbRepository.queryAll(rdbQuery);

        return rdbList.stream().map(DatasetClusterDTO::new).collect(Collectors.toList());
    }

    @Override
    public Page<DatasetInstanceDTO> queryDatasetInstance(final DatasetInstanceDTO instance) {
        RdbInstanceDO query = RdbInstanceDO.builder()
                .rdb(RdbDO.builder().id(instance.getClusterId()).build())
                .host(hostOf(instance.getName()))
                .role(RoleType.MASTER)
                .build();
        Pageable pageable = PagedQuery.ofPage(instance.getCurrent(), instance.getPageSize());
        return rdbInstanceService.query(query, pageable).map(DatasetInstanceDTO::new);
    }

    private String hostOf(final String name) {
        return StringUtils.hasText(name) ? name.split(CommonConstant.COLON)[0] : "";
    }

    @Override
    public void verifyDataset(final List<DataSetDTO> datasets, final DatasetFrom from) {
        verifyProject(datasets);
        Set<Long> tableIds = datasets.stream().map(DataSetDTO::getDataSetId).collect(Collectors.toSet());
        getRdbTables(tableIds);

        if (DatasetFrom.SINK == from) {
            Set<Long> sinkInstanceIds = datasets.stream().map(DataSetDTO::getInstanceId).collect(Collectors.toSet());
            getRdbInstances(sinkInstanceIds);
        }
    }

    @Override
    public Dataset4ConnectionDTO getSourceDataset4Connection(final ConnectionDTO connectionDTO) {
        Long sourceDatasetId = connectionDTO.getSourceDataSetId();
        Long sourceProjectId = connectionDTO.getSourceProjectId();

        RdbTableDTO rdbTableDTO = rdbTableProcessService.getRdbTable(sourceDatasetId);

        RdbDatabaseDTO rdbDatabaseDTO = rdbDatabaseProcessService.getRdbDatabase(rdbTableDTO.getDatabaseId());

        RdbDTO rdbDTO = rdbProcessService.getRdb(rdbDatabaseDTO.getClusterId());

        RdbInstanceDTO rdbInstanceDTO = rdbInstanceProcessService.getDataSourceInstanceByRdbId(rdbDTO.getId());

        ProjectDTO projectDTO = getProject(sourceProjectId);

        return Dataset4ConnectionDTO.builder()
                .dataSystemType(DataSystemType.nameOf(rdbDTO.getRdbType()))

                .projectId(projectDTO.getId())
                .projectName(projectDTO.getName())

                .clusterId(rdbDTO.getId())
                .clusterName(rdbDTO.getName())

                .instanceId(rdbInstanceDTO.getId())
                .instancePort(rdbInstanceDTO.getPort())
                .instanceHost(rdbInstanceDTO.getHost())

                .databaseId(rdbDatabaseDTO.getId())
                .databaseName(rdbDatabaseDTO.getName())

                .dataSetId(rdbTableDTO.getId())
                .datasetName(rdbTableDTO.getName())

                .build();
    }

    @Override
    public Dataset4ConnectionDTO getSinkDataset4Connection(final ConnectionDTO connectionDTO) {
        Long sinkDatasetId = connectionDTO.getSinkDataSetId();
        Long sinkProjectId = connectionDTO.getSinkProjectId();
        Long sinkInstanceId = connectionDTO.getSinkInstanceId();

        RdbTableDTO rdbTableDTO = rdbTableProcessService.getRdbTable(sinkDatasetId);

        RdbDatabaseDTO rdbDatabaseDTO = rdbDatabaseProcessService.getRdbDatabase(rdbTableDTO.getDatabaseId());

        RdbDTO rdbDTO = rdbProcessService.getRdb(rdbDatabaseDTO.getClusterId());

        ProjectDTO projectDTO = getProject(sinkProjectId);

        RdbInstanceDTO rdbInstanceDTO = rdbInstanceProcessService.getRdbInstance(sinkInstanceId);

        return Dataset4ConnectionDTO.builder()
                .dataSystemType(DataSystemType.nameOf(rdbDTO.getRdbType()))

                .projectId(projectDTO.getId())
                .projectName(projectDTO.getName())

                .clusterId(rdbDTO.getId())
                .clusterName(rdbDTO.getName())

                .databaseId(rdbDatabaseDTO.getId())
                .databaseName(rdbDatabaseDTO.getName())

                .dataSetId(rdbTableDTO.getId())
                .datasetName(rdbTableDTO.getName())

                .instanceId(rdbInstanceDTO.getId())
                .instanceHost(rdbInstanceDTO.getHost())
                .instancePort(rdbInstanceDTO.getPort())
                .instanceVIp(rdbInstanceDTO.getVip())
                .build();
    }
}
