package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF
import cn.xdf.acdc.devops.core.domain.entity.HiveSinkConnectorDO;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository for the {@link HiveSinkConnectorDO} entity.
 */
public interface HiveSinkConnectorRepository extends JpaRepository<HiveSinkConnectorDO, Long> {

    /**
     * 根据 source connector 查询对应的sink connector 列表.
     * @param sourceConnectorId  sourceConnectorId
     * @param pageable  pageable
     * @return Page
     */
    @Query(value = "SELECT id,NAME,kafka_topic,cluster_name,database_name,data_set_name FROM (\n"
        + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.data_set_name FROM connector c1,(\n"
        + "SELECT sink_connector.connector_id AS connector_id,kafka_topic.NAME AS kafka_topic,hive_table.NAME AS data_set_name,hive_database.NAME AS database_name,hive.NAME AS cluster_name FROM connector source_connector,source_rdb_table source_rdb_table,kafka_topic kafka_topic,sink_connector sink_connector,hive_sink_connector hive_sink_connector,hive_table hive_table,hive_database hive_database,hive hive WHERE source_connector.id=source_rdb_table.connector_id AND source_rdb_table.kafka_topic_id=kafka_topic.id AND kafka_topic.id=sink_connector.kafka_topic_id AND sink_connector.id=hive_sink_connector.sink_connector_id AND hive_sink_connector.hive_table_id=hive_table.id AND hive_table.hive_database_id=hive_database.id AND hive_database.hive_id=hive.id AND source_connector.id=:sourceConnectorId) c2 WHERE c1.id=c2.connector_id) sink",

        countQuery = "SELECT count(*) FROM (\n"
            + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.data_set_name FROM connector c1,(\n"
            + "SELECT sink_connector.connector_id AS connector_id,kafka_topic.NAME AS kafka_topic,hive_table.NAME AS data_set_name,hive_database.NAME AS database_name,hive.NAME AS cluster_name FROM connector source_connector,source_rdb_table source_rdb_table,kafka_topic kafka_topic,sink_connector sink_connector,hive_sink_connector hive_sink_connector,hive_table hive_table,hive_database hive_database,hive hive WHERE source_connector.id=source_rdb_table.connector_id AND source_rdb_table.kafka_topic_id=kafka_topic.id AND kafka_topic.id=sink_connector.kafka_topic_id AND sink_connector.id=hive_sink_connector.sink_connector_id AND hive_sink_connector.hive_table_id=hive_table.id AND hive_table.hive_database_id=hive_database.id AND hive_database.hive_id=hive.id AND source_connector.id=:sourceConnectorId) c2 WHERE c1.id=c2.connector_id) sink",

        nativeQuery = true)
    Page<Map<String, Object>> findSinkForSource(Pageable pageable, @Param("sourceConnectorId") Long sourceConnectorId);

    Optional<HiveSinkConnectorDO> findByHiveTableId(Long id);

    Optional<HiveSinkConnectorDO> findBySinkConnectorId(Long id);
}
