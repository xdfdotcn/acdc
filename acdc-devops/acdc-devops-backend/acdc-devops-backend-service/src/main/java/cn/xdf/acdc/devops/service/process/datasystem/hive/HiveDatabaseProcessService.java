package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.dto.HiveDatabaseDTO;
import org.springframework.data.domain.Page;

public interface HiveDatabaseProcessService {

    /**
     * 查询hive数据库,分页.
     *
     * @param hiveDatabaseDTO hiveDatabaseDTO
     * @return Page
     */
    Page<HiveDatabaseDTO> queryHiveDatabase(HiveDatabaseDTO hiveDatabaseDTO);

    /**
     * Get hive database.
     *
     * @param id id
     * @return HiveDatabaseDTO
     */
    HiveDatabaseDTO getHiveDatabase(Long id);
}
