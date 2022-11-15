package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the SourceRdbTable entity.
 */
@Repository
public interface SourceRdbTableRepository extends JpaRepository<SourceRdbTableDO, Long>, JpaSpecificationExecutor {

    /**
     * 根据 connectorId 查询.
     * @param connectorId connectorId
     * @return List
     */
    List<SourceRdbTableDO> findOneByConnectorId(Long connectorId);

    /**
     * 根据 rdbTableId 查询.
     * @param rdbTableId rdbTableId
     * @return SourceRdbTable
     */
    Optional<SourceRdbTableDO> findOneByRdbTableId(Long rdbTableId);

    /**
     * 根据 kafkaTopicId 查询.
     * @param kafkaTopicId kafkaTopicId
     * @return SourceRdbTable
     */
    Optional<SourceRdbTableDO> findOneByKafkaTopicId(Long kafkaTopicId);
}
