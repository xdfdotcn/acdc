package cn.xdf.acdc.devops.service.process.datasystem.rdb;

import cn.xdf.acdc.devops.core.domain.dto.RdbDatabaseDTO;
import org.springframework.data.domain.Page;

public interface RdbDatabaseProcessService {

    /**
     * 查询数据库列表,分页.
     *
     * @param rdbDatabaseDTO rdbDatabaseDTO
     * @return Page
     */
    Page<RdbDatabaseDTO> queryRdbDatabase(RdbDatabaseDTO rdbDatabaseDTO);

    /**
     * Get rdb database.
     *
     * @param id id
     * @return RdbDatabaseDTO
     */
    RdbDatabaseDTO getRdbDatabase(Long id);
}
