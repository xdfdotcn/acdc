package cn.xdf.acdc.devops.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnLineageDO;

public interface WideTableSubqueryColumnLineageRepository extends JpaRepository<WideTableSubqueryColumnLineageDO, Long>,
        JpaSpecificationExecutor<WideTableSubqueryColumnLineageDO> {
}
