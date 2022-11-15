package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.Dataset4ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDatabaseDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbTableDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractJdbcDatasetProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbDatabaseProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbInstanceProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbTableProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TidbDatasetProcessServiceImpl extends AbstractJdbcDatasetProcessService {

    @Autowired
    private RdbTableProcessService rdbTableProcessService;

    @Autowired
    private RdbDatabaseProcessService rdbDatabaseProcessService;

    @Autowired
    private RdbProcessService rdbProcessService;

    @Autowired
    private RdbInstanceProcessService rdbInstanceProcessService;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.TIDB;
    }

    @Override
    public Dataset4ConnectionDTO getSourceDataset4Connection(final ConnectionDTO connectionDTO) {
        Long sourceDatasetId = connectionDTO.getSourceDataSetId();
        Long sourceProjectId = connectionDTO.getSourceProjectId();

        RdbTableDTO rdbTableDTO = rdbTableProcessService.getRdbTable(sourceDatasetId);

        RdbDatabaseDTO rdbDatabaseDTO = rdbDatabaseProcessService.getRdbDatabase(rdbTableDTO.getDatabaseId());

        RdbDTO rdbDTO = rdbProcessService.getRdb(rdbDatabaseDTO.getClusterId());

        ProjectDTO projectDTO = getProject(sourceProjectId);

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

                .build();
    }
}
