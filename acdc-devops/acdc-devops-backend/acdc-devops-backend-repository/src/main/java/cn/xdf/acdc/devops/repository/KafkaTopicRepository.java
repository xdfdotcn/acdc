package cn.xdf.acdc.devops.repository;

// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the KafkaTopic entity.
 */
@SuppressWarnings("unused")
@Repository
public interface KafkaTopicRepository extends JpaRepository<KafkaTopicDO, Long>, JpaSpecificationExecutor {

    Optional<KafkaTopicDO> findByKafkaClusterIdAndName(Long kafkaClusterId, String name);
}
