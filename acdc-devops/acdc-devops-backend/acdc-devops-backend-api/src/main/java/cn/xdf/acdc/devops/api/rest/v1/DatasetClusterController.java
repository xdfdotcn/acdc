package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.DatasetClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessServiceManager;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api")
@Transactional
public class DatasetClusterController {

    @Autowired
    private DatasetProcessServiceManager datasetProcessServiceManager;

    /**
     * 查询集群列表.
     * @param datasetClusterDTO clusterDTO
     * @return Page
     */
    @GetMapping("/clusters")
    public PageDTO<DatasetClusterDTO> queryCluster(final DatasetClusterDTO datasetClusterDTO) {

        if (QueryUtil.isNullId(datasetClusterDTO.getProjectId())) {
            return PageDTO.empty();
        }

        List<DatasetProcessService> services = datasetProcessServiceManager.getServices()
            .stream().filter(it -> it.dataSystemType() != DataSystemType.TIDB)
            .collect(Collectors.toList());

        List<DatasetClusterDTO> clusterList = Lists.newArrayList();
        services.forEach(it -> clusterList.addAll(it.queryDatasetCluster(datasetClusterDTO)));
        return PageDTO.of(clusterList, clusterList.size());
    }
}
