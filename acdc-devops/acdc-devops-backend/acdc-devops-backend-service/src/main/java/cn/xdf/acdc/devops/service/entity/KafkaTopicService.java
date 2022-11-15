package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Kafka topic.
 */
public interface KafkaTopicService {

    /**
     * 动态条件.
     *
     * @param kafkaTopic 动态查询 model
     * @return 动态查询条件
     */
    static Specification specificationOf(final KafkaTopicDO kafkaTopic) {
        Preconditions.checkNotNull(kafkaTopic);

        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (Objects.nonNull(kafkaTopic.getKafkaCluster())
                    && Objects.nonNull(kafkaTopic.getKafkaCluster().getId())
            ) {
                predicates.add(cb.equal(root.get("kafkaCluster"), kafkaTopic.getKafkaCluster()));
            }

            if (StringUtils.isNotBlank(kafkaTopic.getName())) {
                predicates.add(cb.like(root.get("name"), QueryUtil.like("%", kafkaTopic.getName(), "%")));
            }

            if (Objects.nonNull(kafkaTopic.getDeleted())) {
                predicates.add(cb.equal(root.get("deleted"), kafkaTopic.getDeleted()));
            }

            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    /**
     * 单条保存.
     *
     * @param kafkaTopic KafkaTopic
     * @return KafkaTopic
     */
    KafkaTopicDO save(KafkaTopicDO kafkaTopic);

    /**
     * 批量创建.
     *
     * @param kafkaTopicList List
     * @return List
     */
    List<KafkaTopicDO> saveAll(List<KafkaTopicDO> kafkaTopicList);

    /**
     * 动态条件分页列表.
     *
     * @param kafkaTopic 查询条件model类
     * @param pageable   分页设置
     * @return 分页列表
     */
    Page<KafkaTopicDO> query(KafkaTopicDO kafkaTopic, Pageable pageable);

    /**
     * 动态条件列表，不分页.
     *
     * @param kafkaTopic kafkaTopic
     * @return 列表数据
     */
    List<KafkaTopicDO> queryAll(KafkaTopicDO kafkaTopic);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return KafkaTopic
     */

    Optional<KafkaTopicDO> findById(Long id);

    /**
     * Batch to obtain.
     *
     * @param ids ids
     * @return List
     */
    List<KafkaTopicDO> findAllById(Iterable<Long> ids);

    /**
     * 根据 kafkaClusterId,name 查询.
     *
     * @param kafkaClusterId kafkaClusterId
     * @param name           name
     * @return KafkaTopic
     */
    Optional<KafkaTopicDO> findByKafkaClusterIdAndName(Long kafkaClusterId, String name);
}
