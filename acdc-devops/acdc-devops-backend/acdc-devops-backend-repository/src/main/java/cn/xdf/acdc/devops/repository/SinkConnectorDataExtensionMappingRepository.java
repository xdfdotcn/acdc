package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDataExtensionMappingDO;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for the {@link SinkConnectorDataExtensionMappingDO} entity.
 */
public interface SinkConnectorDataExtensionMappingRepository extends JpaRepository<SinkConnectorDataExtensionMappingDO, Long> {

    List<SinkConnectorDataExtensionMappingDO> findBySinkConnectorId(Long id);

    void deleteBySinkConnectorId(Long sinkConnectorId);
}
