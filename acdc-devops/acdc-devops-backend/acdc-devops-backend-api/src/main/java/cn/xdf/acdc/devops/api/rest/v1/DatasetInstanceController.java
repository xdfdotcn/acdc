package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.DatasetInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessServiceManager;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api")
@Transactional
public class DatasetInstanceController {

    @Autowired
    private DatasetProcessServiceManager datasetProcessServiceManager;

    /**
     * 查询数据库实例列表.
     *
     * @param datasetInstanceDTO rdbInstanceDTO
     * @return Page
     */
    @GetMapping("/instances")
    public PageDTO<DatasetInstanceDTO> queryRdbInstance(final DatasetInstanceDTO datasetInstanceDTO) {

        if (QueryUtil.isNullId(datasetInstanceDTO.getClusterId())) {
            return PageDTO.empty();
        }

        Page<DatasetInstanceDTO> page = datasetProcessServiceManager
                .getService(datasetInstanceDTO.getDataSystemType())
                .queryDatasetInstance(datasetInstanceDTO);

        return PageDTO.of(page.getContent(), page.getTotalElements());
    }
}
