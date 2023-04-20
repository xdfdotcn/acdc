package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.query.KafkaTopicQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.core.util.QueryUtil;
import com.google.common.base.Strings;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("unused")
@Repository
public interface KafkaTopicRepository extends JpaRepository<KafkaTopicDO, Long>, JpaSpecificationExecutor<KafkaTopicDO> {

    /**
     * Get a kafka topic by kafka cluster id and topic name.
     *
     * @param kafkaClusterId kafka cluster id
     * @param name           topic name
     * @return optinal of kafka topic do
     */
    Optional<KafkaTopicDO> findByKafkaClusterIdAndName(Long kafkaClusterId, String name);

    /**
     * Get a none logical deleted kafka topic by id.
     *
     * @param id kafka topic id
     * @return optinal of kafka topic do
     */
    Optional<KafkaTopicDO> findByDeletedFalseAndId(Long id);

    /**
     * Get a kafka topic by resource id
     *
     * <p>findByDataSystemResourceId: select * from KafkaTopicDO where KafkaTopicDO.dataSystemResource.id=?
     *
     * @param resourceId resource id
     * @return returns Optional if Optional and DO if DO
     */
    Optional<KafkaTopicDO> findByDataSystemResourceId(long resourceId);

    /**
     * Dynamic condition.
     *
     * @param query query
     * @return condition
     */
    default Specification specificationOf(final KafkaTopicQuery query) {

        return (root, criteriaQuery, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.nonNull(query.getKafkaClusterId())
            ) {
                predicates.add(cb.equal(root.get("kafkaCluster").get("id"), query.getKafkaClusterId()));
            }

            if (!Strings.isNullOrEmpty(query.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", query.getName(), "%")));
            }

            if (Objects.nonNull(query.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), query.getDeleted()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * Common query.
     *
     * @param query query
     * @return list
     */
    default List<KafkaTopicDO> query(final KafkaTopicQuery query) {
        return findAll(specificationOf(query));
    }

    /**
     * Paged query.
     *
     * @param query query
     * @return paged list
     */
    default Page<KafkaTopicDO> pagedQuery(final KafkaTopicQuery query) {
        return findAll(specificationOf(query), PagedQuery.pageOf(query));
    }
}
