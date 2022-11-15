package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for the {@link SinkConnectorDO} entity.
 */
public interface SinkConnectorRepository extends JpaRepository<SinkConnectorDO, Long> {

    Optional<SinkConnectorDO> findByConnectorId(Long id);
}
