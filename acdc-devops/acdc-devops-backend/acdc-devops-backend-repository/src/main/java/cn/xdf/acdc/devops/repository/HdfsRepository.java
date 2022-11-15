package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the Hdfs entity.
 */
@SuppressWarnings("unused")
@Repository
public interface HdfsRepository extends JpaRepository<HdfsDO, Long> {

}
