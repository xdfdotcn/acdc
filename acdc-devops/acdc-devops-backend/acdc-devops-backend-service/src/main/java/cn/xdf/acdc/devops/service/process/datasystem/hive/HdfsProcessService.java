package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.dto.HdfsDTO;

public interface HdfsProcessService {

    /**
     * Get hdfs.
     *
     * @param id id
     * @return HdfsDTO
     */
    HdfsDTO getHdfs(Long id);
}
