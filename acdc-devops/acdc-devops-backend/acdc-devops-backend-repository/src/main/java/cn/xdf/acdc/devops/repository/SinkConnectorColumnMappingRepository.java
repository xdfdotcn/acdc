package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the SinkColumnMapping entity.
 */
@SuppressWarnings("unused")
@Repository
public interface SinkConnectorColumnMappingRepository extends JpaRepository<SinkConnectorColumnMappingDO, Long> {

    void deleteBySinkConnectorId(Long sinkConnectorId);

    List<SinkConnectorColumnMappingDO> findBySinkConnectorId(Long sinkConnectorId);
}
