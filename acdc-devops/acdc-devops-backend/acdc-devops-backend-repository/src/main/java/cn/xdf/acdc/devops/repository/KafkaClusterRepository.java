package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Spring Data SQL repository for the KafkaCluster entity.
 */
@SuppressWarnings("unused")
@Repository
public interface KafkaClusterRepository extends JpaRepository<KafkaClusterDO, Long>, JpaSpecificationExecutor<KafkaClusterDO> {

    /**
     * 根据集群类型查询.
     *
     * @param clusterType clusterType
     * @return KafkaCluster
     */
    Optional<KafkaClusterDO> findByClusterType(KafkaClusterType clusterType);

    /**
     * Find by cluster type and bootstrapServers.
     *
     * @param clusterType      clusterType
     * @param bootstrapServers bootstrapServers
     * @return kafka cluster
     */
    Optional<KafkaClusterDO> findByClusterTypeOrBootstrapServers(KafkaClusterType clusterType, String bootstrapServers);

    /**
     * Find by  bootstrapServers.
     *
     * @param bootstrapServers bootstrapServers
     * @return kafka cluster
     */
    Optional<KafkaClusterDO> findByBootstrapServers(String bootstrapServers);

    /**
     * 查询 KafkaCluster 集合.
     *
     * @param query query filter
     * @return Rdb 集合
     */
    default List<KafkaClusterDO> queryAll(final KafkaClusterQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * 分页查询kafka集群列表.
     *
     * @param query    查询条件
     * @param pageable 分页信息
     * @return kafka集群列表
     */
    default Page<KafkaClusterDO> queryAll(final KafkaClusterQuery query, Pageable pageable) {
        return findAll(specificationOf(query), pageable);
    }

    /**
     * Convert query object to specification.
     *
     * @param query query filter
     * @return specification
     */
    default Specification specificationOf(final KafkaClusterQuery query) {
        Preconditions.checkNotNull(query);

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (!CollectionUtils.isEmpty(query.getKafkaClusterIds())) {
                CriteriaBuilder.In in = cb.in(root.get("id"));
                for (Long id : query.getKafkaClusterIds()) {
                    in.value(id);
                }
                predicates.add(in);
            }

            if (StringUtils.isNotBlank(query.getBootstrapServers())) {
                predicates.add(cb.like(root.get("bootstrapServers"), QueryUtil.like("%", query.getBootstrapServers(), "%")));
            }

            if (StringUtils.isNotBlank(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            if (Objects.nonNull(query.getClusterType())) {
                predicates.add(cb.equal(root.get("clusterType"), query.getClusterType()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }
}
