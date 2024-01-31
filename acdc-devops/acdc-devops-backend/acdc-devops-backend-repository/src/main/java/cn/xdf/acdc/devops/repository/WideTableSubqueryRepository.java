package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface WideTableSubqueryRepository extends JpaRepository<WideTableSubqueryDO, Long>,
        JpaSpecificationExecutor<WideTableSubqueryDO> {
}
