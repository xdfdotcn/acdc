package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.util.CollectionUtils;

// CHECKSTYLE:OFF
public interface ConnectionRepository extends JpaRepository<ConnectionDO, Long>,
    JpaSpecificationExecutor<ConnectionDO> {

    /**
     * Get count by a collection of sink dataset id.
     *
     * @param ids ids
     * @return count
     */
    Integer countByDeletedFalseAndSinkDataSetIdIn(Iterable<Long> ids);

    /**
     * Query JDBC connection.
     *
     * @param pageable pageable
     * @param pageable pageable
     * @param isAdmin is admin or not
     * @param userId user Id
     * @return Page
     */
    @Query(value = "SELECT\n"
        + "-- basic\n"
        + "connection.id AS connectionId,\n"
        + "connection.source_connector_id AS sourceConnectorId,\n"
        + "connection.sink_connector_id AS sinkConnectorId,\n"
        + "connection.source_data_system_type AS sourceDataSystemType,\n"
        + "connection.sink_data_system_type AS sinkDataSystemType,\n"
        + "connection.desired_state AS desiredState,\n"
        + "connection.actual_state AS actualState,\n"
        + "connection.requisition_state AS requisitionState,\n"
        + "connection.creation_time AS creationTime,\n"
        + "connection.update_time AS updateTime,\n"
        + "-- id\n"
        + "source_project.id AS sourceProjectId,\n"
        + "sink_project.id AS sinkProjectId,\n"
        + "source_dataset_cluster.id AS sourceDatasetClusterId,\n"
        + "sink_dataset_cluster.id AS sinkDatasetClusterId,\n"
        + "source_dataset_database.id AS sourceDatasetDatabaseId,\n"
        + "sink_dataset_database.id AS sinkDatasetDatabaseId,\n"
        + "source_data_set.id AS sourceDatasetId,\n"
        + "sink_data_set.id AS sinkDatasetId,\n"
        + "sink_dataset_instance.id AS sinkDatasetInstanceId,\n"
        + "-- name\n"
        + "source_project.NAME AS sourceProjectName,\n"
        + "sink_project.NAME AS sinkProjectName,\n"
        + "source_dataset_cluster.NAME AS sourceDatasetClusterName,\n"
        + "sink_dataset_cluster.NAME AS sinkDatasetClusterName,\n"
        + "source_dataset_database.NAME AS sourceDatasetDatabaseName,\n"
        + "sink_dataset_database.NAME AS sinkDatasetDatabaseName,\n"
        + "source_data_set.NAME AS sourceDatasetName,\n"
        + "sink_data_set.NAME AS sinkDatasetName,\n"
        + "sink_dataset_instance.HOST AS sinkDatasetInstanceName\n"
        + "\n"
        + "FROM\n"
        + "\tconnection connection JOIN project source_project ON connection.source_project_id = source_project.id\n"
        + "\tJOIN rdb_table source_data_set ON connection.source_data_set_id = source_data_set.id\n"
        + "\tJOIN rdb_database source_dataset_database ON source_data_set.rdb_database_id = source_dataset_database.id\n"
        + "\tJOIN rdb source_dataset_cluster ON source_dataset_database.rdb_id = source_dataset_cluster.id\n"
        + "\tJOIN project sink_project ON connection.sink_project_id = sink_project.id\n"
        + "\tJOIN rdb_table sink_data_set ON connection.sink_data_set_id = sink_data_set.id\n"
        + "\tJOIN rdb_database sink_dataset_database ON sink_data_set.rdb_database_id = sink_dataset_database.id\n"
        + "\tJOIN rdb sink_dataset_cluster ON sink_dataset_database.rdb_id = sink_dataset_cluster.id\n"
        + "\tJOIN rdb_instance sink_dataset_instance ON connection.sink_instance_id = sink_dataset_instance.id \n"
        + "WHERE\n"
        + "\tconnection.sink_data_system_type =:sinkDataSystemType"
        + " and connection.is_deleted=0"
        + " and if (:isAdmin, 1=1, connection.sink_project_id in (select project_id from rel_project__user where user_id = :userId))"
        + " and IF (IFNULL(:requisitionState,'null')='null', 1=1, connection.requisition_state =:requisitionState)"
        + " and IF (IFNULL(:actualState,'null')='null', 1=1, connection.actual_state =:actualState)"
        + " and IF (IFNULL(:sinkDatasetClusterName,'null')='null', 1=1, sink_dataset_cluster.name like %:sinkDatasetClusterName%)"
        + " and IF (IFNULL(:sinkDatasetDatabaseName,'null')='null', 1=1, sink_dataset_database.name like %:sinkDatasetDatabaseName%)"
        + " and IF (IFNULL(:sinkDatasetName,'null')='null', 1=1, sink_data_set.name like %:sinkDatasetName%)"
        ,
        countQuery = "SELECT\n"
            + "\n"
            + "COUNT(*)\n"
            + "FROM\n"
            + "\tconnection connection JOIN project source_project ON connection.source_project_id = source_project.id\n"
            + "\tJOIN rdb_table source_data_set ON connection.source_data_set_id = source_data_set.id\n"
            + "\tJOIN rdb_database source_dataset_database ON source_data_set.rdb_database_id = source_dataset_database.id\n"
            + "\tJOIN rdb source_dataset_cluster ON source_dataset_database.rdb_id = source_dataset_cluster.id\n"
            + "\tJOIN project sink_project ON connection.sink_project_id = sink_project.id\n"
            + "\tJOIN rdb_table sink_data_set ON connection.sink_data_set_id = sink_data_set.id\n"
            + "\tJOIN rdb_database sink_dataset_database ON sink_data_set.rdb_database_id = sink_dataset_database.id\n"
            + "\tJOIN rdb sink_dataset_cluster ON sink_dataset_database.rdb_id = sink_dataset_cluster.id\n"
            + "\tJOIN rdb_instance sink_dataset_instance ON connection.sink_instance_id = sink_dataset_instance.id \n"
            + "WHERE\n"
            + "\tconnection.sink_data_system_type =:sinkDataSystemType"
            + " and connection.is_deleted=0"
            + " and if (:isAdmin, 1=1, connection.sink_project_id in (select project_id from rel_project__user where user_id = :userId))"
            + " and IF (IFNULL(:requisitionState,'null')='null', 1=1, connection.requisition_state =:requisitionState)"
            + " and IF (IFNULL(:actualState,'null')='null', 1=1, connection.actual_state =:actualState)"
            + " and IF (IFNULL(:sinkDatasetClusterName,'null')='null', 1=1, sink_dataset_cluster.name like %:sinkDatasetClusterName%)"
            + " and IF (IFNULL(:sinkDatasetDatabaseName,'null')='null', 1=1, sink_dataset_database.name like %:sinkDatasetDatabaseName%)"
            + " and IF (IFNULL(:sinkDatasetName,'null')='null', 1=1, sink_data_set.name like %:sinkDatasetName%)"
        ,
        nativeQuery = true)
    Page<Map<String, Object>> findJdbcConnection(
        Pageable pageable,
        @Param("isAdmin") boolean isAdmin,
        @Param("userId") Long userId,
        @Param("requisitionState") Integer requisitionState,
        @Param("actualState") Integer actualState,
        @Param("sinkDatasetClusterName") String sinkDatasetClusterName,
        @Param("sinkDatasetDatabaseName") String sinkDatasetDatabaseName,
        @Param("sinkDatasetName") String sinkDatasetName,
        @Param("sinkDataSystemType") int sinkDataSystemType
    );

    @Query(value = "SELECT \n"
        + "-- basic\n"
        + "connection.id AS connectionId,\n"
        + "connection.source_connector_id AS sourceConnectorId,\n"
        + "connection.sink_connector_id AS sinkConnectorId,\n"
        + "connection.source_data_system_type AS sourceDataSystemType,\n"
        + "connection.sink_data_system_type AS sinkDataSystemType,\n"
        + "connection.desired_state AS desiredState,\n"
        + "connection.actual_state AS actualState,\n"
        + "connection.requisition_state AS requisitionState,\n"
        + "connection.creation_time AS creationTime,\n"
        + "connection.update_time AS updateTime,\n"
        + "-- id\n"
        + "source_project.id AS sourceProjectId,\n"
        + "sink_project.id AS sinkProjectId,\n"
        + "source_dataset_cluster.id AS sourceDatasetClusterId,\n"
        + "sink_dataset_cluster.id AS sinkDatasetClusterId,\n"
        + "source_dataset_database.id AS sourceDatasetDatabaseId,\n"
        + "sink_dataset_database.id AS sinkDatasetDatabaseId,\n"
        + "source_data_set.id AS sourceDatasetId,\n"
        + "sink_data_set.id AS sinkDatasetId,\n"
        + "-1 AS sinkDatasetInstanceId,\n"
        + "-- name\n"
        + "source_project.NAME AS sourceProjectName,\n"
        + "sink_project.NAME AS sinkProjectName,\n"
        + "source_dataset_cluster.NAME AS sourceDatasetClusterName,\n"
        + "sink_dataset_cluster.NAME AS sinkDatasetClusterName,\n"
        + "source_dataset_database.NAME AS sourceDatasetDatabaseName,\n"
        + "sink_dataset_database.NAME AS sinkDatasetDatabaseName,\n"
        + "source_data_set.NAME AS sourceDatasetName,\n"
        + "sink_data_set.NAME AS sinkDatasetName,\n"
        + "'' AS sinkDatasetInstanceName\n"
        + "\n"
        + "FROM\n"
        + "\tconnection connection JOIN project source_project ON connection.source_project_id = source_project.id\n"
        + "\tJOIN rdb_table source_data_set ON connection.source_data_set_id = source_data_set.id\n"
        + "\tJOIN rdb_database source_dataset_database ON source_data_set.rdb_database_id = source_dataset_database.id\n"
        + "\tJOIN rdb source_dataset_cluster ON source_dataset_database.rdb_id = source_dataset_cluster.id\n"
        + "\tJOIN project sink_project ON connection.sink_project_id = sink_project.id\n"
        + "\tJOIN hive_table sink_data_set ON connection.sink_data_set_id = sink_data_set.id\n"
        + "\tJOIN hive_database sink_dataset_database ON sink_data_set.hive_database_id = sink_dataset_database.id\n"
        + "\tJOIN hive sink_dataset_cluster ON sink_dataset_database.hive_id = sink_dataset_cluster.id\n"
        + "WHERE\n"
        + "\tconnection.sink_data_system_type =2"
        + " and connection.is_deleted=0"
        + " and if (:isAdmin, 1=1, connection.sink_project_id in (select project_id from rel_project__user where user_id = :userId))"
        + " and IF (IFNULL(:requisitionState,'null')='null', 1=1, connection.requisition_state =:requisitionState)"
        + " and IF (IFNULL(:actualState,'null')='null', 1=1, connection.actual_state =:actualState)"
        + " and IF (IFNULL(:sinkDatasetClusterName,'null')='null', 1=1, sink_dataset_cluster.name like %:sinkDatasetClusterName%)"
        + " and IF (IFNULL(:sinkDatasetDatabaseName,'null')='null', 1=1, sink_dataset_database.name like %:sinkDatasetDatabaseName%)"
        + " and IF (IFNULL(:sinkDatasetName,'null')='null', 1=1, sink_data_set.name like %:sinkDatasetName%)"
        ,
        countQuery = "SELECT \n"
            + "COUNT(*)\n"
            + "FROM\n"
            + "\tconnection connection JOIN project source_project ON connection.source_project_id = source_project.id\n"
            + "\tJOIN rdb_table source_data_set ON connection.source_data_set_id = source_data_set.id\n"
            + "\tJOIN rdb_database source_dataset_database ON source_data_set.rdb_database_id = source_dataset_database.id\n"
            + "\tJOIN rdb source_dataset_cluster ON source_dataset_database.rdb_id = source_dataset_cluster.id\n"
            + "\tJOIN project sink_project ON connection.sink_project_id = sink_project.id\n"
            + "\tJOIN hive_table sink_data_set ON connection.sink_data_set_id = sink_data_set.id\n"
            + "\tJOIN hive_database sink_dataset_database ON sink_data_set.hive_database_id = sink_dataset_database.id\n"
            + "\tJOIN hive sink_dataset_cluster ON sink_dataset_database.hive_id = sink_dataset_cluster.id\n"
            + "WHERE\n"
            + "\tconnection.sink_data_system_type =2"
            + " and connection.is_deleted=0"
            + " and if (:isAdmin, 1=1, connection.sink_project_id in (select project_id from rel_project__user where user_id = :userId))"
            + " and IF (IFNULL(:requisitionState,'null')='null', 1=1, connection.requisition_state =:requisitionState)"
            + " and IF (IFNULL(:actualState,'null')='null', 1=1, connection.actual_state =:actualState)"
            + " and IF (IFNULL(:sinkDatasetClusterName,'null')='null', 1=1, sink_dataset_cluster.name like %:sinkDatasetClusterName%)"
            + " and IF (IFNULL(:sinkDatasetDatabaseName,'null')='null', 1=1, sink_dataset_database.name like %:sinkDatasetDatabaseName%)"
            + " and IF (IFNULL(:sinkDatasetName,'null')='null', 1=1, sink_data_set.name like %:sinkDatasetName%)"

        ,
        nativeQuery = true)
    Page<Map<String, Object>> findHiveConnection(
        Pageable pageable,
        @Param("isAdmin") boolean isAdmin,
        @Param("userId") Long userId,
        @Param("requisitionState") Integer requisitionState,
        @Param("actualState") Integer actualState,
        @Param("sinkDatasetClusterName") String sinkDatasetClusterName,
        @Param("sinkDatasetDatabaseName") String sinkDatasetDatabaseName,
        @Param("sinkDatasetName") String sinkDatasetName
    );


    @Query(value = "SELECT \n"
        + "-- basic\n"
        + "connection.id AS connectionId,\n"
        + "connection.source_connector_id AS sourceConnectorId,\n"
        + "connection.sink_connector_id AS sinkConnectorId,\n"
        + "connection.source_data_system_type AS sourceDataSystemType,\n"
        + "connection.sink_data_system_type AS sinkDataSystemType,\n"
        + "connection.desired_state AS desiredState,\n"
        + "connection.actual_state AS actualState,\n"
        + "connection.requisition_state AS requisitionState,\n"
        + "connection.creation_time AS creationTime,\n"
        + "connection.update_time AS updateTime,\n"
        + "-- id\n"
        + "source_project.id AS sourceProjectId,\n"
        + "sink_project.id AS sinkProjectId,\n"
        + "source_dataset_cluster.id AS sourceDatasetClusterId,\n"
        + "sink_dataset_cluster.id AS sinkDatasetClusterId,\n"
        + "source_dataset_database.id AS sourceDatasetDatabaseId,\n"
        + "-1 AS sinkDatasetDatabaseId,\n"
        + "source_data_set.id AS sourceDatasetId,\n"
        + "sink_data_set.id AS sinkDatasetId,\n"
        + "-1 AS sinkDatasetInstanceId,\n"
        + "-- name\n"
        + "source_project.NAME AS sourceProjectName,\n"
        + "sink_project.NAME AS sinkProjectName,\n"
        + "source_dataset_cluster.NAME AS sourceDatasetClusterName,\n"
        + "sink_dataset_cluster.NAME AS sinkDatasetClusterName,\n"
        + "source_dataset_database.NAME AS sourceDatasetDatabaseName,\n"
        + "'' AS sinkDatasetDatabaseName,\n"
        + "source_data_set.NAME AS sourceDatasetName,\n"
        + "sink_data_set.NAME AS sinkDatasetName,\n"
        + "'' AS sinkDatasetInstanceName\n"
        + "\n"
        + "FROM\n"
        + "\tconnection connection JOIN project source_project ON connection.source_project_id = source_project.id\n"
        + "\tJOIN rdb_table source_data_set ON connection.source_data_set_id = source_data_set.id\n"
        + "\tJOIN rdb_database source_dataset_database ON source_data_set.rdb_database_id = source_dataset_database.id\n"
        + "\tJOIN rdb source_dataset_cluster ON source_dataset_database.rdb_id = source_dataset_cluster.id\n"
        + "\tJOIN project sink_project ON connection.sink_project_id = sink_project.id\n"
        + "\tJOIN kafka_topic sink_data_set ON connection.sink_data_set_id = sink_data_set.id\n"
        + "\tJOIN kafka_cluster sink_dataset_cluster ON sink_data_set.kafka_cluster_id = sink_dataset_cluster.id\n"
        + "WHERE\n"
        + "\tconnection.sink_data_system_type =3"
        + " and connection.is_deleted=0"
        + " and if (:isAdmin, 1=1, connection.sink_project_id in (select project_id from rel_project__user where user_id = :userId))"
        + " and IF (IFNULL(:requisitionState,'null')='null', 1=1, connection.requisition_state =:requisitionState)"
        + " and IF (IFNULL(:actualState,'null')='null', 1=1, connection.actual_state =:actualState)"
        + " and IF (IFNULL(:sinkDatasetClusterName,'null')='null', 1=1, sink_dataset_cluster.name like %:sinkDatasetClusterName%)"
        + " and IF (IFNULL(:sinkDatasetName,'null')='null', 1=1, sink_data_set.name like %:sinkDatasetName%)"
        ,
        countQuery = "SELECT \n"
            + "COUNT(*)\n"
            + "FROM\n"
            + "\tconnection connection JOIN project source_project ON connection.source_project_id = source_project.id\n"
            + "\tJOIN rdb_table source_data_set ON connection.source_data_set_id = source_data_set.id\n"
            + "\tJOIN rdb_database source_dataset_database ON source_data_set.rdb_database_id = source_dataset_database.id\n"
            + "\tJOIN rdb source_dataset_cluster ON source_dataset_database.rdb_id = source_dataset_cluster.id\n"
            + "\tJOIN project sink_project ON connection.sink_project_id = sink_project.id\n"
            + "\tJOIN kafka_topic sink_data_set ON connection.sink_data_set_id = sink_data_set.id\n"
            + "\tJOIN kafka_cluster sink_dataset_cluster ON sink_data_set.kafka_cluster_id = sink_dataset_cluster.id\n"
            + "WHERE\n"
            + "\tconnection.sink_data_system_type =3"
            + " and connection.is_deleted=0"
            + " and if (:isAdmin, 1=1, connection.sink_project_id in (select project_id from rel_project__user where user_id = :userId))"
            + " and IF (IFNULL(:requisitionState,'null')='null', 1=1, connection.requisition_state =:requisitionState)"
            + " and IF (IFNULL(:actualState,'null')='null', 1=1, connection.actual_state =:actualState)"
            + " and IF (IFNULL(:sinkDatasetClusterName,'null')='null', 1=1, sink_dataset_cluster.name like %:sinkDatasetClusterName%)"
            + " and IF (IFNULL(:sinkDatasetName,'null')='null', 1=1, sink_data_set.name like %:sinkDatasetName%)"
        ,
        nativeQuery = true)
    Page<Map<String, Object>> findKafkaConnection(
        Pageable pageable,
        @Param("isAdmin") boolean isAdmin,
        @Param("userId") Long userId,
        @Param("requisitionState") Integer requisitionState,
        @Param("actualState") Integer actualState,
        @Param("sinkDatasetClusterName") String sinkDatasetClusterName,
        @Param("sinkDatasetName") String sinkDatasetName
    );

    default List<ConnectionDO> query(ConnectionQuery query) {
        return findAll(specificationOf(query));
    }

    static Specification specificationOf(final ConnectionQuery connectionQuery) {
        Preconditions.checkNotNull(connectionQuery);
        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (!CollectionUtils.isEmpty(connectionQuery.getConnectionIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : connectionQuery.getConnectionIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (Objects.nonNull(connectionQuery.getRequisitionState())) {
                predicates.add(cb.equal(root.get("requisitionState"), connectionQuery.getRequisitionState()));
            }

            if (Objects.nonNull(connectionQuery.getConnectionId())) {
                predicates.add(cb.equal(root.get("id"), connectionQuery.getConnectionId()));
            }
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
