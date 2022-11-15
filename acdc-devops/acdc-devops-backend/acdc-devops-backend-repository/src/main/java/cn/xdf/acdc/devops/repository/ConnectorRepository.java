package cn.xdf.acdc.devops.repository;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import java.util.Map;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the Connector entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorRepository extends JpaRepository<ConnectorDO, Long>, JpaSpecificationExecutor {

    /**
     * 根据 source connector 查询对应的sink connector 列表.
     * @param id  source connector ID
     * @param pageable  pageable
     * @return Page
     */
    @Query(value = "SELECT id,NAME,kafka_topic,cluster_name,database_name,table_name FROM (\n"
        + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.table_name FROM connector c1,(\n"
        + "SELECT sink_t.connector_id AS connector_id,kt.NAME AS kafka_topic,tb.NAME AS table_name,db.NAME AS database_name,cluster.NAME AS cluster_name FROM connector source_c,source_rdb_table source_t,kafka_topic kt,sink_rdb_table sink_t,rdb_table tb,rdb_database db,rdb cluster WHERE source_c.id=source_t.connector_id AND source_t.kafka_topic_id=kt.id AND source_t.kafka_topic_id=sink_t.kafka_topic_id AND sink_t.rdb_table_id=tb.id AND tb.rdb_database_id=db.id AND db.rdb_id=cluster.id AND source_c.id=:id) c2 WHERE c1.id=c2.connector_id UNION ALL \n"
        + "SELECT c1.id,c1.NAME,c2.kafka_topic,c2.cluster_name,c2.database_name,c2.table_name FROM connector c1,(\n"
        + "SELECT sink_t.connector_id AS connector_id,kt.NAME AS kafka_topic,tb.NAME AS table_name,db.NAME AS database_name,cluster.NAME AS cluster_name FROM connector source_c,source_rdb_table source_t,kafka_topic kt,sink_hive_table sink_t,hive_table tb,hive_database db,hive cluster WHERE source_c.id=source_t.connector_id AND source_t.kafka_topic_id=kt.id AND source_t.kafka_topic_id=sink_t.kafka_topic_id AND sink_t.hive_table_id=tb.id AND tb.hive_database_id=db.id AND db.hive_id=cluster.id AND source_c.id=:id) c2 WHERE c1.id=c2.connector_id) sink_link",

        countQuery = "SELECT count(*) FROM (\n"
            + "SELECT c1.id FROM connector c1,(\n"
            + "SELECT sink_t.connector_id AS connector_id FROM connector source_c,source_rdb_table source_t,kafka_topic kt,sink_rdb_table sink_t,rdb_table tb,rdb_database db,rdb cluster WHERE source_c.id=source_t.connector_id AND source_t.kafka_topic_id=kt.id AND source_t.kafka_topic_id=sink_t.kafka_topic_id AND sink_t.rdb_table_id=tb.id AND tb.rdb_database_id=db.id AND db.rdb_id=cluster.id AND source_c.id=:id) c2 WHERE c1.id=c2.connector_id UNION ALL \n"
            + "SELECT c1.id FROM connector c1,(\n"
            + "SELECT sink_t.connector_id AS connector_id FROM connector source_c,source_rdb_table source_t,kafka_topic kt,sink_hive_table sink_t,hive_table tb,hive_database db,hive cluster WHERE source_c.id=source_t.connector_id AND source_t.kafka_topic_id=kt.id AND source_t.kafka_topic_id=sink_t.kafka_topic_id AND sink_t.hive_table_id=tb.id AND tb.hive_database_id=db.id AND db.hive_id=cluster.id AND source_c.id=:id) c2 WHERE c1.id=c2.connector_id) sink_link",
        nativeQuery = true)
    Page<Map<String, Object>> findBySourceConnectorId(Pageable pageable, @Param("id") Long id);
}
