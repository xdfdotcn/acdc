package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import java.util.List;
import java.util.Optional;

/**
 * Hdfs name node.
 */
public interface HdfsNamenodeService {

    /**
     * 保存 Hdfs namenode.
     *
     * @param hdfsNamenode Hdfs namenode
     * @return 保存成功的 Hdfs集群
     */
    HdfsNamenodeDO save(HdfsNamenodeDO hdfsNamenode);

    /**
     * 查询所有的Hdfs namenode.
     *
     * @return 全部的Hdfs集群列表
     */
    List<HdfsNamenodeDO> findAll();

    /**
     * 根据 ID 查询 hdfs namenode.
     *
     * @param id id
     * @return HdfsNamenode
     */
    Optional<HdfsNamenodeDO> findById(Long id);
}
