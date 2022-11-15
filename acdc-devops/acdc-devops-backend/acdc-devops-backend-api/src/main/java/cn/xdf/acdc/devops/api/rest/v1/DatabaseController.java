package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.HiveDatabaseDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDatabaseDTO;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDatabaseProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbDatabaseProcessService;
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
public class DatabaseController {

    @Autowired
    private RdbDatabaseProcessService rdbDatabaseProcessService;

    @Autowired
    private HiveDatabaseProcessService hiveDatabaseProcessService;

    /**
     * 查询 Rdb 数据库列表.
     * @param rdbDatabaseDTO rdbDatabaseDTO
     * @return Page
     */
    @GetMapping("/databases/rdbDatabases")
    public PageDTO<RdbDTO> queryRdbDatabase(final RdbDatabaseDTO rdbDatabaseDTO) {
        if (QueryUtil.isNullId(rdbDatabaseDTO.getClusterId())) {
            return PageDTO.empty();
        }
        Page page = rdbDatabaseProcessService.queryRdbDatabase(rdbDatabaseDTO);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }

    /**
     * 查询 hive 数据库列表.
     * @param hiveDatabase hiveDatabase
     * @return Page
     */
    @GetMapping("/databases/hiveDatabases")
    public PageDTO<HiveDatabaseDTO> queryHiveDatabase(final HiveDatabaseDTO hiveDatabase) {
        if (QueryUtil.isNullId(hiveDatabase.getClusterId())) {
            return PageDTO.empty();
        }
        Page page = hiveDatabaseProcessService.queryHiveDatabase(hiveDatabase);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }
}
