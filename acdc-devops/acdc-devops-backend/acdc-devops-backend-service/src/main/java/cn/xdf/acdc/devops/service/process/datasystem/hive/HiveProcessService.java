package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.dto.HiveDTO;

public interface HiveProcessService {

    /**
     * Get hive.
     *
     * @param id id
     * @return HiveDTO
     */
    HiveDTO getHive(Long id);
}
