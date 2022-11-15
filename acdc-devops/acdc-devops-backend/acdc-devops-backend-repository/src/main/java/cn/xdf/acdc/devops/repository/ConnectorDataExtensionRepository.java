package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the ConnectorDataExtension entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorDataExtensionRepository extends JpaRepository<ConnectorDataExtensionDO, Long> {

    /**
     * 根据主键集合批量删除.
     * @param ids 主键集合
     */
    void deleteByIdIn(List<Long> ids);
}
