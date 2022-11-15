package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.dto.HiveTableDTO;
import org.springframework.data.domain.Page;

public interface HiveTableProcessService {

    /**
     * 查询hive数据表,分页.
     * @param hiveTableDTO hiveTableDTO
     * @return Page
     */
    Page<HiveTableDTO> queryHiveTable(HiveTableDTO hiveTableDTO);

    /**
     * Get hive table.
     * @param id id
     * @return HiveTableDTO
     */
    HiveTableDTO getHiveTable(Long id);
}
