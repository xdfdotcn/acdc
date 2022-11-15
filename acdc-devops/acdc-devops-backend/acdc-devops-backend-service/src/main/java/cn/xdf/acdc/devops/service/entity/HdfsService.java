package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import java.util.List;

/**
 * Hdfs.
 */
public interface HdfsService {

    /**
     * 保存 Hdfs集群.
     *
     * @param hdfs Hdfs集群
     * @return 保存成功的 Hdfs集群
     */
    HdfsDO save(HdfsDO hdfs);

    /**
     * 查询所有的Hdfs集群.
     *
     * @return 全部的Hdfs集群列表
     */
    List<HdfsDO> findAll();
}
