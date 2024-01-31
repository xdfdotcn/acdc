package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface WideTableSubqueryColumnRepository extends JpaRepository<WideTableSubqueryColumnDO, Long>,
        JpaSpecificationExecutor<WideTableSubqueryColumnDO> {
}
