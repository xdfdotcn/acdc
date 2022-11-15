package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.HiveTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbTableDTO;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveTableProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbTableProcessService;
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
public class TableController {

    @Autowired
    private RdbTableProcessService rdbTableProcessService;

    @Autowired
    private HiveTableProcessService hiveTableProcessService;

    /**
     * 查询 Rdb 数据表列表.
     * @param rdbTableDTO rdbTableDTO
     * @return Page
     */
    @GetMapping("/tables/rdbTables")
    public PageDTO<RdbTableDTO> queryRdbTable(final RdbTableDTO rdbTableDTO) {
        if (QueryUtil.isNullId(rdbTableDTO.getDatabaseId())) {
            return PageDTO.empty();
        }
        Page page = rdbTableProcessService.queryRdbTable(rdbTableDTO);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }


    /**
     * 查询 hive 数据表列表.
     * @param hiveTable hiveTable
     * @return Page
     */
    @GetMapping("/tables/hiveTables")
    public PageDTO<HiveTableDTO> queryRdbTable(final HiveTableDTO hiveTable) {
        if (QueryUtil.isNullId(hiveTable.getDatabaseId())) {
            return PageDTO.empty();
        }
        Page page = hiveTableProcessService.queryHiveTable(hiveTable);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }
}
