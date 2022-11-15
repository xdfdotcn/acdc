package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.Dataset4ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.DatasetInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.DatasetFrom;
import org.springframework.data.domain.Page;

import java.util.List;

public interface DatasetProcessService extends DataSystemTypeService {

    /**
     * Query instance.
     * @param instance instance
     * @return Page
     */
    Page<DatasetInstanceDTO> queryDatasetInstance(DatasetInstanceDTO instance);

    /**
     * Query cluster.
     * @param cluster cluster
     * @return List
     */
    List<DatasetClusterDTO> queryDatasetCluster(DatasetClusterDTO cluster);

    /**
     * Check dataset exists.
     * @param datasets datasets
     * @param from  sink or source
     */
    void verifyDataset(List<DataSetDTO> datasets, DatasetFrom from);

    /**
     * Get dataset for source connection.
     * @param connectionDTO  connectionDTO
     * @return dataset
     */
    Dataset4ConnectionDTO getSourceDataset4Connection(ConnectionDTO connectionDTO);

    /**
     * Get dataset for sink connection .
     * @param connectionDTO  connectionDTO
     * @return dataset
     */
    Dataset4ConnectionDTO getSinkDataset4Connection(ConnectionDTO connectionDTO);
}
