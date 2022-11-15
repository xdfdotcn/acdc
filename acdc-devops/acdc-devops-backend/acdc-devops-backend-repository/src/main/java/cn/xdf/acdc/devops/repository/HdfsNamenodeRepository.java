package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the HdfsNamenode entity.
 */
@SuppressWarnings("unused")
@Repository
public interface HdfsNamenodeRepository extends JpaRepository<HdfsNamenodeDO, Long> {

}
