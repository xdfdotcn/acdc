package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF
import cn.xdf.acdc.devops.core.domain.entity.KafkaSinkConnectorDO;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository for the {@link KafkaSinkConnectorDO} entity.
 */
public interface KafkaSinkConnectorRepository extends JpaRepository<KafkaSinkConnectorDO, Long> {

    /**
     * 根据 source connector 查询对应的sink connector 列表.
     * @param sourceConnectorId  sourceConnectorId
     * @param pageable  pageable
     * @return Page
     */
    @Query(value = "SELECT id,NAME,kafka_topic,cluster_name,database_name,data_set_name FROM (\n"
        + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.data_set_name FROM connector c1,(\n"
        + "SELECT sink_connector.connector_id AS connector_id,kafka_topic.NAME AS kafka_topic,data_set.NAME AS data_set_name,'' AS database_name,kafka_cluster.bootstrap_servers AS cluster_name FROM connector source_connector,source_rdb_table source_rdb_table,kafka_topic kafka_topic,sink_connector sink_connector,kafka_sink_connector kafka_sink_connector,kafka_topic data_set,kafka_cluster kafka_cluster WHERE source_connector.id=source_rdb_table.connector_id AND source_rdb_table.kafka_topic_id=kafka_topic.id AND kafka_topic.id=sink_connector.kafka_topic_id AND sink_connector.id=kafka_sink_connector.sink_connector_id AND kafka_sink_connector.kafka_topic_id=data_set.id AND data_set.kafka_cluster_id=kafka_cluster.id AND source_connector.id=:sourceConnectorId) c2 WHERE c1.id=c2.connector_id) sink",

        countQuery = "SELECT count(*) FROM (\n"
            + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.data_set_name FROM connector c1,(\n"
            + "SELECT sink_connector.connector_id AS connector_id,kafka_topic.NAME AS kafka_topic,data_set.NAME AS data_set_name,'' AS database_name,kafka_cluster.bootstrap_servers AS cluster_name FROM connector source_connector,source_rdb_table source_rdb_table,kafka_topic kafka_topic,sink_connector sink_connector,kafka_sink_connector kafka_sink_connector,kafka_topic data_set,kafka_cluster kafka_cluster WHERE source_connector.id=source_rdb_table.connector_id AND source_rdb_table.kafka_topic_id=kafka_topic.id AND kafka_topic.id=sink_connector.kafka_topic_id AND sink_connector.id=kafka_sink_connector.sink_connector_id AND kafka_sink_connector.kafka_topic_id=data_set.id AND data_set.kafka_cluster_id=kafka_cluster.id AND source_connector.id=:sourceConnectorId) c2 WHERE c1.id=c2.connector_id) sink",

        nativeQuery = true)
    Page<Map<String, Object>> findSinkForSource(Pageable pageable, @Param("sourceConnectorId") Long sourceConnectorId);

    Optional<KafkaSinkConnectorDO> findBySinkConnectorId(Long id);

    Optional<KafkaSinkConnectorDO> findByKafkaTopicId(Long id);
}
