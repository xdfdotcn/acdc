package cn.xdf.acdc.devops.service.process.datasystem.rdb;

import cn.xdf.acdc.devops.core.domain.dto.RdbTableDTO;
import org.springframework.data.domain.Page;

public interface RdbTableProcessService {

    /**
     * 查询数据表列表,分页.
     *
     * @param rdbTableDTO rdbTableDTO
     * @return Page
     */
    Page<RdbTableDTO> queryRdbTable(RdbTableDTO rdbTableDTO);

    /**
     * Get rdb table.
     *
     * @param id id
     * @return RdbTableDTO
     */
    RdbTableDTO getRdbTable(Long id);
}
