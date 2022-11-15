package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF
import cn.xdf.acdc.devops.core.domain.entity.JdbcSinkConnectorDO;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository for the {@link JdbcSinkConnectorDO} entity.
 */
public interface JdbcSinkConnectorRepository extends JpaRepository<JdbcSinkConnectorDO, Long> {

    /**
     * 根据 source connector 查询对应的sink connector 列表.
     * @param sourceConnectorId  sourceConnectorId
     * @param pageable  pageable
     * @return Page
     */
    @Query(value = "SELECT id,NAME,kafka_topic,cluster_name,database_name,data_set_name FROM (\n"
        + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.data_set_name FROM connector c1,(\n"
        + "SELECT sink_connector.connector_id AS connector_id,kafka_topic.NAME AS kafka_topic,rdb_table.NAME AS data_set_name,rdb_database.NAME AS database_name,rdb.NAME AS cluster_name FROM connector source_connector,source_rdb_table source_rdb_table,kafka_topic kafka_topic,sink_connector sink_connector,jdbc_sink_connector jdbc_sink_connector,rdb_table rdb_table,rdb_database rdb_database,rdb rdb WHERE source_connector.id=source_rdb_table.connector_id AND source_rdb_table.kafka_topic_id=kafka_topic.id AND kafka_topic.id=sink_connector.kafka_topic_id AND sink_connector.id=jdbc_sink_connector.sink_connector_id AND jdbc_sink_connector.rdb_table_id=rdb_table.id AND rdb_table.rdb_database_id=rdb_database.id AND rdb_database.rdb_id=rdb.id AND source_connector.id=:sourceConnectorId) c2 WHERE c1.id=c2.connector_id) sink",

        countQuery = "SELECT count(*) FROM (\n"
            + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.data_set_name FROM connector c1,(\n"
            + "SELECT sink_connector.connector_id AS connector_id,kafka_topic.NAME AS kafka_topic,rdb_table.NAME AS data_set_name,rdb_database.NAME AS database_name,rdb.NAME AS cluster_name FROM connector source_connector,source_rdb_table source_rdb_table,kafka_topic kafka_topic,sink_connector sink_connector,jdbc_sink_connector jdbc_sink_connector,rdb_table rdb_table,rdb_database rdb_database,rdb rdb WHERE source_connector.id=source_rdb_table.connector_id AND source_rdb_table.kafka_topic_id=kafka_topic.id AND kafka_topic.id=sink_connector.kafka_topic_id AND sink_connector.id=jdbc_sink_connector.sink_connector_id AND jdbc_sink_connector.rdb_table_id=rdb_table.id AND rdb_table.rdb_database_id=rdb_database.id AND rdb_database.rdb_id=rdb.id AND source_connector.id=:sourceConnectorId) c2 WHERE c1.id=c2.connector_id) sink",

        nativeQuery = true)
    Page<Map<String, Object>> findSinkForSource(Pageable pageable, @Param("sourceConnectorId") Long sourceConnectorId);

    Optional<JdbcSinkConnectorDO> findBySinkConnectorId(Long id);

    Optional<JdbcSinkConnectorDO> findByRdbTableId(Long id);
}
