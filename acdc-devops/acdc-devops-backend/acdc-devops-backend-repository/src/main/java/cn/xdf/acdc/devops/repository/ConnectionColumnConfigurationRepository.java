package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;

public interface ConnectionColumnConfigurationRepository extends JpaRepository<ConnectionColumnConfigurationDO, Long>, JpaSpecificationExecutor<ConnectionColumnConfigurationDO> {
    
    /**
     * delete by connection ids.
     *
     * @param connectionIds connection ids
     */
    void deleteByConnectionIdIn(List<Long> connectionIds);
}
